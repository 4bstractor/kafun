defmodule KafunTest do
  use ExUnit.Case, async: false

  alias Kafun.{GC, Index, Multipart, Storage, S3XML}

  describe "Auth.SigV4 (signing)" do
    alias Kafun.Auth.SigV4

    @fixed_now ~U[2026-05-02 12:00:00Z]

    test "produces a structurally valid Authorization header with all required parts" do
      headers =
        SigV4.sign(:get, "https://example.com/bucket/key", [],
          access_key: "AKIA0",
          secret_key: "secret-shhh",
          payload: {:hash, ""},
          now: @fixed_now
        )

      auth = Enum.find_value(headers, fn {k, v} -> if k == "authorization", do: v end)
      assert auth =~ ~r/^AWS4-HMAC-SHA256 Credential=AKIA0\/20260502\/us-east-1\/s3\/aws4_request, SignedHeaders=[a-z0-9;\-]+, Signature=[a-f0-9]{64}$/

      assert {"x-amz-date", "20260502T120000Z"} in headers
      assert Enum.any?(headers, fn {k, _} -> k == "x-amz-content-sha256" end)
      assert Enum.any?(headers, fn {k, _} -> k == "host" end)
    end

    test "is deterministic — same inputs yield byte-identical signature" do
      args = [
        access_key: "AKIA0",
        secret_key: "secret-shhh",
        payload: {:hash, "hello"},
        now: @fixed_now
      ]

      h1 = SigV4.sign(:put, "https://example.com/b/k", [{"content-type", "text/plain"}], args)
      h2 = SigV4.sign(:put, "https://example.com/b/k", [{"content-type", "text/plain"}], args)

      assert auth_of(h1) == auth_of(h2)
    end

    test "different bodies produce different signatures (hash payload)" do
      base = [
        access_key: "AKIA0",
        secret_key: "secret-shhh",
        now: @fixed_now
      ]

      h_a = SigV4.sign(:put, "https://example.com/b/k", [], [{:payload, {:hash, "A"}} | base])
      h_b = SigV4.sign(:put, "https://example.com/b/k", [], [{:payload, {:hash, "B"}} | base])
      refute auth_of(h_a) == auth_of(h_b)
    end

    test "UNSIGNED-PAYLOAD signs even with no body access" do
      headers =
        SigV4.sign(:put, "https://example.com/b/k", [],
          access_key: "AKIA0",
          secret_key: "secret-shhh",
          payload: :unsigned,
          now: @fixed_now
        )

      assert {"x-amz-content-sha256", "UNSIGNED-PAYLOAD"} in headers
    end

    defp auth_of(headers), do: Enum.find_value(headers, fn {k, v} -> if k == "authorization", do: v end)
  end

  describe "Auth.SigV4 (verifying)" do
    alias Kafun.Auth.SigV4

    @fixed_now ~U[2026-05-05 12:00:00Z]

    # Build a conn that mimics what Bandit hands Plug for a freshly-signed request.
    # Plug.Test defaults conn.host to "www.example.com"; we sign against the
    # same host so the canonical request matches.
    defp signed_conn(method, path, query, body, opts) do
      url = "http://www.example.com#{path}#{if query == "", do: "", else: "?" <> query}"
      payload = Keyword.get(opts, :payload, {:hash, body || ""})

      headers =
        SigV4.sign(method, url, [],
          access_key: Keyword.fetch!(opts, :access_key),
          secret_key: Keyword.fetch!(opts, :secret_key),
          payload: payload,
          now: Keyword.get(opts, :now, @fixed_now)
        )

      conn = Plug.Test.conn(method, path <> if(query == "", do: "", else: "?" <> query), body || "")

      # `Plug.Conn.put_req_header` refuses "host"; bypass via struct mutation
      # for that one header (Bandit puts it in req_headers in production).
      Enum.reduce(headers, conn, fn {k, v}, c ->
        name = String.downcase(k)

        if name == "host" do
          %{c | req_headers: [{"host", v} | c.req_headers]}
        else
          Plug.Conn.put_req_header(c, name, v)
        end
      end)
    end

    defp lookup(known) do
      fn id ->
        case Map.fetch(known, id) do
          :error -> :not_found
          {:ok, :revoked} -> :revoked
          {:ok, :empty} -> :empty_secret
          {:ok, secret} when is_binary(secret) -> {:ok, secret}
        end
      end
    end

    test "verify accepts a freshly-signed request with the matching secret" do
      conn = signed_conn(:get, "/wallpapers", "", "",
        access_key: "AKID0", secret_key: "supersecret"
      )

      assert {:ok, :verified, "AKID0"} = SigV4.verify(conn, lookup(%{"AKID0" => "supersecret"}))
    end

    test "verify rejects when secret is wrong" do
      conn = signed_conn(:get, "/wallpapers", "", "",
        access_key: "AKID1", secret_key: "actual-secret"
      )

      assert {:error, :invalid_signature} =
               SigV4.verify(conn, lookup(%{"AKID1" => "wrong-secret"}))
    end

    test "verify rejects when the request body or headers were tampered after signing" do
      conn = signed_conn(:put, "/b/k", "", "original",
        access_key: "AKID2", secret_key: "s"
      )

      # Mutate a signed header to simulate tampering.
      tampered = Plug.Conn.put_req_header(conn, "x-amz-date", "19700101T000000Z")

      assert {:error, :invalid_signature} = SigV4.verify(tampered, lookup(%{"AKID2" => "s"}))
    end

    test "verify returns :unknown_key when the access key isn't in the lookup" do
      conn = signed_conn(:get, "/b", "", "",
        access_key: "AKID-UNKNOWN", secret_key: "x"
      )

      assert {:error, :unknown_key} = SigV4.verify(conn, lookup(%{}))
    end

    test "verify returns :revoked_key on a revoked-key sentinel" do
      conn = signed_conn(:get, "/b", "", "", access_key: "AKID-DEAD", secret_key: "x")

      assert {:error, :revoked_key} =
               SigV4.verify(conn, lookup(%{"AKID-DEAD" => :revoked}))
    end

    test "verify returns :unverified for the legacy empty-secret bootstrap path" do
      # Sign with any old secret — verify shouldn't even check it because
      # the lookup returns :empty_secret.
      conn = signed_conn(:get, "/b", "", "", access_key: "ENV-KEY", secret_key: "ignored")

      assert {:ok, :unverified, "ENV-KEY"} =
               SigV4.verify(conn, lookup(%{"ENV-KEY" => :empty}))
    end

    test "verify returns :no_credentials when there's no auth header or querystring" do
      conn = Plug.Test.conn(:get, "/b")
      assert {:error, :no_credentials} = SigV4.verify(conn, lookup(%{}))
    end

    test "verify rejects streaming-signed payloads" do
      conn = signed_conn(:put, "/b/k", "", "body", access_key: "K", secret_key: "S")

      streamed =
        Plug.Conn.put_req_header(
          conn,
          "x-amz-content-sha256",
          "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
        )

      assert {:error, :stream_signed_payload} =
               SigV4.verify(streamed, lookup(%{"K" => "S"}))
    end

    test "verify accepts a request with querystring credentials as :unverified" do
      # Mirrors the admin UI's image-preview trick.
      conn =
        Plug.Test.conn(
          :get,
          "/wallpapers/foo.png?X-Amz-Credential=AKID0/admin/us-east-1/s3/aws4_request"
        )

      assert {:ok, :unverified, "AKID0"} =
               SigV4.verify(conn, lookup(%{"AKID0" => "supersecret"}))
    end
  end

  describe "Auth.authorize/2 (gate)" do
    alias Kafun.Auth
    alias Kafun.Auth.SigV4

    @fixed_now ~U[2026-05-05 12:00:00Z]

    setup do
      # Test env defaults `auth_disabled?: true` so existing unsigned-conn
      # tests keep working. This describe block is the gate's own tests —
      # it needs to enforce auth to actually validate behavior.
      previous = Application.get_env(:kafun, :auth_disabled?, false)
      Application.put_env(:kafun, :auth_disabled?, false)

      tmp = Path.join(System.tmp_dir!(), "kafun-authgate-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      start_supervised!({Index, db_path: db})

      on_exit(fn ->
        Application.put_env(:kafun, :auth_disabled?, previous)
        File.rm_rf!(tmp)
      end)

      :ok
    end

    defp signed_conn_for(method, path, opts) do
      url = "http://www.example.com#{path}"
      payload = Keyword.get(opts, :payload, {:hash, ""})

      headers =
        SigV4.sign(method, url, [],
          access_key: Keyword.fetch!(opts, :access_key),
          secret_key: Keyword.fetch!(opts, :secret_key),
          payload: payload,
          now: Keyword.get(opts, :now, @fixed_now)
        )

      conn = Plug.Test.conn(method, path, "")

      Enum.reduce(headers, conn, fn {k, v}, c ->
        name = String.downcase(k)

        if name == "host" do
          %{c | req_headers: [{"host", v} | c.req_headers]}
        else
          Plug.Conn.put_req_header(c, name, v)
        end
      end)
    end

    test "authenticated read with read grant succeeds" do
      :ok = Index.create_access_key("KEY1", "secret", "")
      :ok = Index.upsert_grant("KEY1", "wallpapers", :read)
      :ok = Index.ensure_bucket("wallpapers")

      conn = signed_conn_for(:get, "/wallpapers", access_key: "KEY1", secret_key: "secret")
      assert :ok = Auth.authorize(conn, action: :read, bucket: "wallpapers")
    end

    test "read grant is insufficient for write action" do
      :ok = Index.create_access_key("KEY2", "secret", "")
      :ok = Index.upsert_grant("KEY2", "b", :read)

      conn = signed_conn_for(:put, "/b/k", access_key: "KEY2", secret_key: "secret")
      assert {:error, :forbidden} = Auth.authorize(conn, action: :write, bucket: "b")
    end

    test "write grant satisfies read and write but not admin" do
      :ok = Index.create_access_key("KEY3", "secret", "")
      :ok = Index.upsert_grant("KEY3", "b", :write)

      conn_r = signed_conn_for(:get, "/b", access_key: "KEY3", secret_key: "secret")
      assert :ok = Auth.authorize(conn_r, action: :read, bucket: "b")

      conn_w = signed_conn_for(:put, "/b/k", access_key: "KEY3", secret_key: "secret")
      assert :ok = Auth.authorize(conn_w, action: :write, bucket: "b")

      conn_a = signed_conn_for(:delete, "/b", access_key: "KEY3", secret_key: "secret")
      assert {:error, :forbidden} = Auth.authorize(conn_a, action: :admin, bucket: "b")
    end

    test "admin grant satisfies all actions" do
      :ok = Index.create_access_key("KEY4", "secret", "")
      :ok = Index.upsert_grant("KEY4", "b", :admin)

      for action <- [:read, :write, :admin] do
        method = if action == :read, do: :get, else: :put
        conn = signed_conn_for(method, "/b", access_key: "KEY4", secret_key: "secret")
        assert :ok = Auth.authorize(conn, action: action, bucket: "b")
      end
    end

    test "global '*' grant covers buckets that lack a specific grant" do
      :ok = Index.create_access_key("KEY5", "secret", "")
      :ok = Index.upsert_grant("KEY5", "*", :write)

      conn = signed_conn_for(:put, "/random-bucket/k", access_key: "KEY5", secret_key: "secret")
      assert :ok = Auth.authorize(conn, action: :write, bucket: "random-bucket")
    end

    test "anonymous + public bucket + :read → :ok" do
      :ok = Index.ensure_bucket("public-pile")
      :ok = Index.set_bucket_public_read("public-pile", true)

      conn = Plug.Test.conn(:get, "/public-pile/k")
      assert :ok = Auth.authorize(conn, action: :read, bucket: "public-pile")
    end

    test "anonymous + public bucket + :write → :unauthenticated" do
      :ok = Index.ensure_bucket("public-pile")
      :ok = Index.set_bucket_public_read("public-pile", true)

      conn = Plug.Test.conn(:put, "/public-pile/k")
      assert {:error, :unauthenticated} = Auth.authorize(conn, action: :write, bucket: "public-pile")
    end

    test "anonymous + private bucket → :unauthenticated" do
      :ok = Index.ensure_bucket("private-pile")
      conn = Plug.Test.conn(:get, "/private-pile/k")
      assert {:error, :unauthenticated} = Auth.authorize(conn, action: :read, bucket: "private-pile")
    end

    test "unknown key in the credential → :unknown_key" do
      conn = signed_conn_for(:get, "/b", access_key: "GHOST", secret_key: "x")
      assert {:error, :unknown_key} = Auth.authorize(conn, action: :read, bucket: "b")
    end

    test "revoked key → :revoked_key" do
      :ok = Index.create_access_key("KEY6", "secret", "")
      :ok = Index.revoke_access_key("KEY6")

      conn = signed_conn_for(:get, "/b", access_key: "KEY6", secret_key: "secret")
      assert {:error, :revoked_key} = Auth.authorize(conn, action: :read, bucket: "b")
    end

    test "invalid signature (wrong secret on client side) → :invalid_signature" do
      :ok = Index.create_access_key("KEY7", "real-secret", "")
      :ok = Index.upsert_grant("KEY7", "b", :read)

      conn = signed_conn_for(:get, "/b", access_key: "KEY7", secret_key: "wrong-client-secret")
      assert {:error, :invalid_signature} = Auth.authorize(conn, action: :read, bucket: "b")
    end

    test "streaming-signed payload header → :stream_signed_payload" do
      :ok = Index.create_access_key("KEY8", "s", "")
      :ok = Index.upsert_grant("KEY8", "b", :write)

      conn =
        signed_conn_for(:put, "/b/k", access_key: "KEY8", secret_key: "s")
        |> Plug.Conn.put_req_header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")

      assert {:error, :stream_signed_payload} =
               Auth.authorize(conn, action: :write, bucket: "b")
    end

    test "empty-secret legacy key with a grant → :ok (signature skipped)" do
      :ok = Index.create_access_key("ENV-KEY", "", "env bootstrap")
      :ok = Index.upsert_grant("ENV-KEY", "*", :admin)

      conn = signed_conn_for(:get, "/anywhere", access_key: "ENV-KEY", secret_key: "anything")
      assert :ok = Auth.authorize(conn, action: :read, bucket: "anywhere")
    end

    test "authorize_service :list_buckets allows anonymous and any key" do
      conn_anon = Plug.Test.conn(:get, "/")
      assert {:ok, :anonymous} = Auth.authorize_service(conn_anon, action: :list_buckets)

      :ok = Index.create_access_key("KEY9", "s", "")
      conn_keyed = signed_conn_for(:get, "/", access_key: "KEY9", secret_key: "s")
      assert {:ok, "KEY9"} = Auth.authorize_service(conn_keyed, action: :list_buckets)
    end

    test "authorize_service :create_bucket requires a global admin grant" do
      :ok = Index.create_access_key("KEY10", "s", "")
      :ok = Index.upsert_grant("KEY10", "specific", :admin)

      conn = signed_conn_for(:put, "/new-bucket", access_key: "KEY10", secret_key: "s")
      # specific bucket admin doesn't suffice for CreateBucket
      assert {:error, :forbidden} = Auth.authorize_service(conn, action: :create_bucket)

      :ok = Index.upsert_grant("KEY10", "*", :admin)
      assert {:ok, "KEY10"} = Auth.authorize_service(conn, action: :create_bucket)
    end

    test "accessible_buckets returns the right scope per caller" do
      :ok = Index.ensure_bucket("a-public")
      :ok = Index.set_bucket_public_read("a-public", true)
      :ok = Index.ensure_bucket("b-private")
      :ok = Index.ensure_bucket("c-private")

      # Anonymous: only the public one.
      assert ["a-public"] = Auth.accessible_buckets(:anonymous)

      # Specific-bucket grants for a key.
      :ok = Index.create_access_key("KEY11", "s", "")
      :ok = Index.upsert_grant("KEY11", "b-private", :read)
      assert ["b-private"] = Auth.accessible_buckets("KEY11")

      # Global '*' grant: every bucket.
      :ok = Index.upsert_grant("KEY11", "*", :read)
      bs = Auth.accessible_buckets("KEY11") |> Enum.sort()
      assert bs == ["a-public", "b-private", "c-private"]
    end
  end

  describe "Router auth gate integration" do
    alias Kafun.Auth.SigV4

    @fixed_now ~U[2026-05-05 12:00:00Z]

    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-router-auth-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)

      # Disable the test escape hatch for THIS describe block — we want the
      # real gate to fire so we can validate the wiring.
      previous = Application.get_env(:kafun, :auth_disabled?, false)
      Application.put_env(:kafun, :auth_disabled?, false)

      start_supervised!({Index, db_path: db})

      on_exit(fn ->
        Application.put_env(:kafun, :auth_disabled?, previous)
        File.rm_rf!(tmp)
      end)

      %{root: tmp}
    end

    defp signed_router_conn(method, path, opts) do
      url = "http://www.example.com#{path}"
      payload = Keyword.get(opts, :payload, {:hash, ""})

      headers =
        SigV4.sign(method, url, [],
          access_key: Keyword.fetch!(opts, :access_key),
          secret_key: Keyword.fetch!(opts, :secret_key),
          payload: payload,
          now: Keyword.get(opts, :now, @fixed_now)
        )

      conn = Plug.Test.conn(method, path, "")

      Enum.reduce(headers, conn, fn {k, v}, c ->
        name = String.downcase(k)

        if name == "host" do
          %{c | req_headers: [{"host", v} | c.req_headers]}
        else
          Plug.Conn.put_req_header(c, name, v)
        end
      end)
    end

    test "anonymous GET on a private bucket returns 403 AccessDenied" do
      :ok = Index.ensure_bucket("private-b")

      conn = Plug.Test.conn(:get, "/private-b") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 403
      assert conn.resp_body =~ "AccessDenied"
    end

    test "anonymous GET on a public-read bucket succeeds" do
      :ok = Index.ensure_bucket("public-b")
      :ok = Index.set_bucket_public_read("public-b", true)

      conn =
        Plug.Test.conn(:get, "/public-b?list-type=2") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "anonymous PUT on a public-read bucket is still 403 (writes never anonymous)" do
      :ok = Index.ensure_bucket("public-b")
      :ok = Index.set_bucket_public_read("public-b", true)

      conn =
        Plug.Test.conn(:put, "/public-b/k", "x") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 403
    end

    test "signed GET with a read grant succeeds" do
      :ok = Index.ensure_bucket("test-bucket")
      :ok = Index.create_access_key("READER", "secret", "")
      :ok = Index.upsert_grant("READER", "test-bucket", :read)

      conn =
        signed_router_conn(:get, "/test-bucket?list-type=2",
          access_key: "READER",
          secret_key: "secret"
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "signed PUT object with only a :read grant is 403 forbidden" do
      :ok = Index.ensure_bucket("test-bucket")
      :ok = Index.create_access_key("READER", "secret", "")
      :ok = Index.upsert_grant("READER", "test-bucket", :read)

      conn =
        signed_router_conn(:put, "/test-bucket/k",
          access_key: "READER",
          secret_key: "secret",
          payload: {:hash, ""}
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 403
      assert conn.resp_body =~ "AccessDenied"
    end

    test "signed PUT with a :write grant succeeds" do
      :ok = Index.ensure_bucket("test-bucket")
      :ok = Index.create_access_key("WRITER", "secret", "")
      :ok = Index.upsert_grant("WRITER", "test-bucket", :write)

      conn =
        signed_router_conn(:put, "/test-bucket/k",
          access_key: "WRITER",
          secret_key: "secret",
          payload: {:hash, ""}
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "CreateBucket without a global admin grant is 403" do
      :ok = Index.create_access_key("LOCAL_ADMIN", "secret", "")
      :ok = Index.upsert_grant("LOCAL_ADMIN", "specific", :admin)

      conn =
        signed_router_conn(:put, "/new-bucket",
          access_key: "LOCAL_ADMIN",
          secret_key: "secret"
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 403
    end

    test "CreateBucket with a global admin grant succeeds" do
      :ok = Index.create_access_key("GLOBAL_ADMIN", "secret", "")
      :ok = Index.upsert_grant("GLOBAL_ADMIN", "*", :admin)

      conn =
        signed_router_conn(:put, "/new-bucket",
          access_key: "GLOBAL_ADMIN",
          secret_key: "secret"
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "ListAllMyBuckets returns only buckets the caller can access" do
      :ok = Index.ensure_bucket("granted")
      :ok = Index.ensure_bucket("denied")
      :ok = Index.create_access_key("LIMITED", "secret", "")
      :ok = Index.upsert_grant("LIMITED", "granted", :read)

      conn =
        signed_router_conn(:get, "/", access_key: "LIMITED", secret_key: "secret")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<Name>granted</Name>"
      refute conn.resp_body =~ "<Name>denied</Name>"
    end

    test "env-bootstrapped key with empty secret and global admin works without sig verification" do
      :ok = Index.ensure_bucket("any")
      :ok = Index.create_access_key("ENV_KEY", "", "env-bootstrap")
      :ok = Index.upsert_grant("ENV_KEY", "*", :admin)

      # Sign with a bogus secret — verifier sees :empty_secret and skips check.
      conn =
        signed_router_conn(:get, "/any?list-type=2",
          access_key: "ENV_KEY",
          secret_key: "anything"
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "revoked key returns 403 InvalidAccessKeyId" do
      :ok = Index.ensure_bucket("test-bucket")
      :ok = Index.create_access_key("DEAD", "secret", "")
      :ok = Index.upsert_grant("DEAD", "*", :admin)
      :ok = Index.revoke_access_key("DEAD")

      conn =
        signed_router_conn(:get, "/test-bucket", access_key: "DEAD", secret_key: "secret")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 403
      assert conn.resp_body =~ "InvalidAccessKeyId"
    end
  end

  describe "Migrate end-to-end via two Bandit instances over loopback" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-mig-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      Application.put_env(:kafun, :allowed_keys, MapSet.new([]))
      start_supervised!({Index, db_path: db})

      # Two Bandit instances on auto-allocated ports both serve Kafun.Router.
      # In real deploys src and dst are different services on different hosts;
      # for the test, sharing the storage backend is fine because we use
      # different bucket names for src/dst — the migration is observable as
      # objects landing under the dst bucket.
      {:ok, src_pid} = Bandit.start_link(plug: Kafun.Router, port: 0, ip: {127, 0, 0, 1})
      {:ok, dst_pid} = Bandit.start_link(plug: Kafun.Router, port: 0, ip: {127, 0, 0, 1})

      src_port = port_of(src_pid)
      dst_port = port_of(dst_pid)

      on_exit(fn ->
        Process.exit(src_pid, :normal)
        Process.exit(dst_pid, :normal)
        File.rm_rf!(tmp)
      end)

      %{
        src_url: "http://127.0.0.1:#{src_port}",
        dst_url: "http://127.0.0.1:#{dst_port}"
      }
    end

    test "copies a populated src bucket into a fresh dst bucket", %{src_url: src_url, dst_url: dst_url} do
      # Populate a "from-seaweed" bucket via the router directly.
      Plug.Test.conn(:put, "/from-seaweed") |> Kafun.Router.call(Kafun.Router.init([]))

      for {k, body} <- [{"a/1", "alpha"}, {"a/2", "bravo"}, {"b/1", "charlie"}] do
        Plug.Test.conn(:put, "/from-seaweed/#{k}", body)
        |> Plug.Conn.put_req_header("content-type", "text/plain")
        |> Kafun.Router.call(Kafun.Router.init([]))
      end

      src = Kafun.Migrate.client(src_url, "AKIA0", "")
      dst = Kafun.Migrate.client(dst_url, "AKIA0", "")

      summary =
        Kafun.Migrate.run(src, dst, "from-seaweed", dst_bucket: "into-kafun", concurrency: 4)

      assert summary.copied == 3
      assert summary.skipped == 0
      assert summary.failed == 0
      assert summary.bytes == byte_size("alpha") + byte_size("bravo") + byte_size("charlie")

      # All three objects land under the renamed dst bucket with the right bytes.
      for {k, body} <- [{"a/1", "alpha"}, {"a/2", "bravo"}, {"b/1", "charlie"}] do
        assert {:ok, %{size: size, etag: etag}} = Index.get("into-kafun", k)
        assert size == byte_size(body)
        assert etag == :crypto.hash(:md5, body) |> Base.encode16(case: :lower)
      end
    end

    test "is idempotent: re-running skips already-copied objects",
         %{src_url: src_url, dst_url: dst_url} do
      Plug.Test.conn(:put, "/seaweed-bucket") |> Kafun.Router.call(Kafun.Router.init([]))
      Plug.Test.conn(:put, "/seaweed-bucket/k", "first") |> Kafun.Router.call(Kafun.Router.init([]))

      src = Kafun.Migrate.client(src_url, "AKIA0", "")
      dst = Kafun.Migrate.client(dst_url, "AKIA0", "")

      first = Kafun.Migrate.run(src, dst, "seaweed-bucket", dst_bucket: "kafun-bucket")
      assert first.copied == 1

      second = Kafun.Migrate.run(src, dst, "seaweed-bucket", dst_bucket: "kafun-bucket")
      assert second.copied == 0
      assert second.skipped == 1
    end

    test "dry_run reports counts without writing to the destination",
         %{src_url: src_url, dst_url: dst_url} do
      Plug.Test.conn(:put, "/dry-src") |> Kafun.Router.call(Kafun.Router.init([]))
      Plug.Test.conn(:put, "/dry-src/k", "x") |> Kafun.Router.call(Kafun.Router.init([]))

      src = Kafun.Migrate.client(src_url, "AKIA0", "")
      dst = Kafun.Migrate.client(dst_url, "AKIA0", "")

      summary = Kafun.Migrate.run(src, dst, "dry-src", dst_bucket: "dry-dst", dry_run: true)
      assert summary.copied == 1

      # Destination bucket still doesn't exist.
      refute Index.bucket_exists?("dry-dst")
    end

    test "preserves user metadata (x-amz-meta-*) across the copy",
         %{src_url: src_url, dst_url: dst_url} do
      Plug.Test.conn(:put, "/meta-src") |> Kafun.Router.call(Kafun.Router.init([]))

      Plug.Test.conn(:put, "/meta-src/m", "with-meta")
      |> Plug.Conn.put_req_header("x-amz-meta-author", "naka")
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> Kafun.Router.call(Kafun.Router.init([]))

      src = Kafun.Migrate.client(src_url, "AKIA0", "")
      dst = Kafun.Migrate.client(dst_url, "AKIA0", "")
      Kafun.Migrate.run(src, dst, "meta-src", dst_bucket: "meta-dst")

      {:ok, meta} = Index.get("meta-dst", "m")
      assert meta.content_type == "text/plain"
      assert Map.get(meta.meta, "author") == "naka"
    end

    defp port_of(pid) do
      info = ThousandIsland.listener_info(pid)

      case info do
        {:ok, {_ip, port}} -> port
        port when is_integer(port) -> port
      end
    end
  end

  describe "Storage.valid_key?/1 — path traversal protection" do
    test "rejects empty / oversize / control bytes" do
      refute Storage.valid_key?("")
      refute Storage.valid_key?(String.duplicate("a", 1025))
      refute Storage.valid_key?("foo\nbar")
      refute Storage.valid_key?("foo\rbar")
      refute Storage.valid_key?("foo" <> <<0>> <> "bar")
    end

    test "rejects keys that would traverse out of the bucket dir" do
      refute Storage.valid_key?("../escape")
      refute Storage.valid_key?("a/../b")
      refute Storage.valid_key?("foo/../../etc/passwd")
      refute Storage.valid_key?("./hidden")
      refute Storage.valid_key?("/abs/path")
      refute Storage.valid_key?("..")
      refute Storage.valid_key?(".")
    end

    test "accepts ordinary keys including dots inside segments" do
      assert Storage.valid_key?("foo")
      assert Storage.valid_key?("foo/bar")
      assert Storage.valid_key?("images/2024/photo.jpg")
      assert Storage.valid_key?("foo..bar")
      assert Storage.valid_key?(".hidden-but-not-traversal")
      assert Storage.valid_key?("a..b/c")
    end
  end

  describe "Storage.parse_range/2" do
    test "absent / empty returns :none" do
      assert Storage.parse_range(nil, 100) == :none
      assert Storage.parse_range("", 100) == :none
    end

    test "open-ended" do
      assert Storage.parse_range("bytes=0-", 100) == {:ok, 0, 99}
      assert Storage.parse_range("bytes=50-", 100) == {:ok, 50, 99}
    end

    test "suffix range" do
      assert Storage.parse_range("bytes=-10", 100) == {:ok, 90, 99}
    end

    test "explicit window, clamped to size" do
      assert Storage.parse_range("bytes=10-20", 100) == {:ok, 10, 20}
      assert Storage.parse_range("bytes=10-200", 100) == {:ok, 10, 99}
    end

    test "invalid forms" do
      assert Storage.parse_range("bytes=abc", 100) == :invalid
      assert Storage.parse_range("bytes=200-300", 100) == :invalid
    end
  end

  describe "Index.upper_bound/1" do
    test "increments the last byte" do
      assert Index.upper_bound("foo") == "fop"
      assert Index.upper_bound("a") == "b"
    end

    test "carries over 0xFF" do
      assert Index.upper_bound(<<0x66, 0xFF>>) == <<0x67>>
    end

    test "all-0xFF prefix has no upper bound" do
      assert Index.upper_bound(<<0xFF, 0xFF>>) == nil
    end

    test "empty prefix returns nil" do
      assert Index.upper_bound("") == nil
    end
  end

  describe "S3XML" do
    test "list_all_buckets renders" do
      xml =
        S3XML.list_all_buckets([
          %{name: "imouto", created_at: 0},
          %{name: "wallpapers", created_at: 0}
        ])
        |> IO.iodata_to_binary()

      assert xml =~ "<Bucket><Name>imouto</Name>"
      assert xml =~ "<Bucket><Name>wallpapers</Name>"
    end

    test "list_objects renders truncation token" do
      xml =
        S3XML.list_objects(
          "imouto",
          "",
          nil,
          1000,
          [%{key: "a", size: 1, etag: "x", mtime: 0}],
          [],
          true,
          "a"
        )
        |> IO.iodata_to_binary()

      assert xml =~ "<IsTruncated>true</IsTruncated>"
      assert xml =~ "<NextContinuationToken>"
      assert xml =~ "<ETag>\"x\"</ETag>"
    end

    test "list_objects renders common prefixes when delimiter set" do
      xml =
        S3XML.list_objects(
          "imouto",
          "",
          "/",
          1000,
          [],
          ["a/", "b/"],
          false,
          nil
        )
        |> IO.iodata_to_binary()

      assert xml =~ "<Delimiter>/</Delimiter>"
      assert xml =~ "<CommonPrefixes><Prefix>a/</Prefix></CommonPrefixes>"
      assert xml =~ "<CommonPrefixes><Prefix>b/</Prefix></CommonPrefixes>"
      assert xml =~ "<KeyCount>2</KeyCount>"
    end

    test "error escapes the resource" do
      xml =
        S3XML.error("NoSuchKey", "missing", "/foo&bar")
        |> IO.iodata_to_binary()

      assert xml =~ "/foo&amp;bar"
    end
  end

  describe "Vault (encryption primitives)" do
    alias Kafun.Vault

    setup do
      on_exit(fn -> Application.delete_env(:kafun, :master_key) end)
      :ok
    end

    test "disabled vault passes secrets through untouched" do
      refute Vault.enabled?()
      assert Vault.encrypt("hunter2") == "hunter2"
      assert Vault.decrypt("hunter2") == "hunter2"
    end

    test "round-trips under a master key; ciphertext carries the version prefix" do
      Application.put_env(:kafun, :master_key, "correct horse battery staple")
      stored = Vault.encrypt("hunter2")

      assert Vault.encrypted?(stored)
      refute stored =~ "hunter2"
      assert Vault.decrypt(stored) == "hunter2"
    end

    test "empty secret is never encrypted — it is the unverified-mode sentinel" do
      Application.put_env(:kafun, :master_key, "some master key")
      assert Vault.encrypt("") == ""
    end

    test "fails closed: wrong master key or tampered row returns the ciphertext" do
      Application.put_env(:kafun, :master_key, "key A")
      stored = Vault.encrypt("hunter2")

      Application.put_env(:kafun, :master_key, "key B")
      assert Vault.decrypt(stored) == stored

      Application.put_env(:kafun, :master_key, "key A")
      tampered = "enc:v1:" <> Base.encode64(:crypto.strong_rand_bytes(40))
      assert Vault.decrypt(tampered) == tampered
    end

    test "fails closed when the master key disappears" do
      Application.put_env(:kafun, :master_key, "key A")
      stored = Vault.encrypt("hunter2")

      Application.delete_env(:kafun, :master_key)
      assert Vault.decrypt(stored) == stored
    end
  end

  describe "Vault ↔ Index (encryption at rest)" do
    alias Kafun.Vault

    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-vault-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      start_supervised!({Index, db_path: db})

      on_exit(fn ->
        Application.delete_env(:kafun, :master_key)
        File.rm_rf!(tmp)
      end)

      :ok
    end

    test "secrets land encrypted on disk but read back as plaintext" do
      Application.put_env(:kafun, :master_key, "master-1")
      :ok = Index.create_access_key("VKEY", "topsecret", "")

      assert {:ok, %{secret: "topsecret"}} = Index.get_access_key("VKEY")

      # Peek under the vault: with the key gone, the stored value surfaces.
      Application.delete_env(:kafun, :master_key)
      assert {:ok, %{secret: stored}} = Index.get_access_key("VKEY")
      assert Vault.encrypted?(stored)
    end

    test "encrypt_plaintext_secrets sweeps legacy plaintext rows, skips empty" do
      :ok = Index.create_access_key("PLAIN", "legacy-secret", "")
      :ok = Index.create_access_key("EMPTY", "", "env-bootstrap")

      assert Index.encrypt_plaintext_secrets() == 0

      Application.put_env(:kafun, :master_key, "master-1")
      assert Index.encrypt_plaintext_secrets() == 1
      assert Index.encrypt_plaintext_secrets() == 0

      assert {:ok, %{secret: "legacy-secret"}} = Index.get_access_key("PLAIN")
      assert {:ok, %{secret: ""}} = Index.get_access_key("EMPTY")
    end

    test "rekey_secrets rotates master keys all-or-nothing" do
      Application.put_env(:kafun, :master_key, "master-old")
      :ok = Index.create_access_key("RK1", "alpha", "")
      :ok = Index.create_access_key("RK2", "beta", "")

      Application.put_env(:kafun, :master_key, "master-new")
      assert {:error, {:undecryptable, ids}} = Index.rekey_secrets("not-the-old-key")
      assert Enum.sort(ids) == ["RK1", "RK2"]

      assert {:ok, 2} = Index.rekey_secrets("master-old")
      assert {:ok, %{secret: "alpha"}} = Index.get_access_key("RK1")
      assert {:ok, %{secret: "beta"}} = Index.get_access_key("RK2")
    end

    test "rekey_secrets with the vault disabled rewrites back to plaintext" do
      Application.put_env(:kafun, :master_key, "master-old")
      :ok = Index.create_access_key("RK3", "gamma", "")

      Application.delete_env(:kafun, :master_key)
      assert {:ok, 1} = Index.rekey_secrets("master-old")

      assert {:ok, %{secret: "gamma"}} = Index.get_access_key("RK3")
    end

    test "SigV4 gate still authorizes with encrypted secrets" do
      alias Kafun.Auth.SigV4

      Application.put_env(:kafun, :master_key, "master-1")
      previous = Application.get_env(:kafun, :auth_disabled?, false)
      Application.put_env(:kafun, :auth_disabled?, false)
      on_exit(fn -> Application.put_env(:kafun, :auth_disabled?, previous) end)

      :ok = Index.create_access_key("SIGKEY", "sigsecret", "")
      :ok = Index.upsert_grant("SIGKEY", "vaultbucket", :write)

      headers =
        SigV4.sign(:put, "http://www.example.com/vaultbucket/k.txt", [],
          access_key: "SIGKEY",
          secret_key: "sigsecret",
          payload: {:hash, ""},
          now: DateTime.utc_now()
        )

      conn =
        Enum.reduce(headers, Plug.Test.conn(:put, "/vaultbucket/k.txt", ""), fn {k, v}, c ->
          name = String.downcase(k)

          if name == "host" do
            %{c | req_headers: [{"host", v} | c.req_headers]}
          else
            Plug.Conn.put_req_header(c, name, v)
          end
        end)

      assert :ok = Kafun.Auth.authorize(conn, action: :write, bucket: "vaultbucket")
    end
  end

  describe "Index access keys + grants" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-acl-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      :ok
    end

    test "create / get / list / revoke an access key" do
      :ok = Index.create_access_key("AKID0", "secret0", "test key")
      assert {:ok, %{id: "AKID0", secret: "secret0", description: "test key", status: :active}} =
               Index.get_access_key("AKID0")

      assert [%{id: "AKID0", status: :active}] = Index.list_access_keys()

      :ok = Index.revoke_access_key("AKID0")
      assert {:ok, %{status: :revoked, revoked_at: ts}} = Index.get_access_key("AKID0")
      assert is_integer(ts)
    end

    test "revoke / set_secret / set_description return :not_found on unknown id" do
      assert :not_found = Index.revoke_access_key("ghost")
      assert :not_found = Index.set_access_key_secret("ghost", "x")
      assert :not_found = Index.set_access_key_description("ghost", "x")
      assert :not_found = Index.get_access_key("ghost")
    end

    test "create is idempotent — re-creating an existing id is a no-op" do
      :ok = Index.create_access_key("AKID1", "first", "first version")
      :ok = Index.create_access_key("AKID1", "second", "shouldn't replace")

      assert {:ok, %{secret: "first", description: "first version"}} =
               Index.get_access_key("AKID1")
    end

    test "set_access_key_secret rotates the secret in place" do
      :ok = Index.create_access_key("AKID2", "old", "")
      :ok = Index.set_access_key_secret("AKID2", "new")
      assert {:ok, %{secret: "new"}} = Index.get_access_key("AKID2")
    end

    test "upsert_grant + list_bucket_grants + list_grants_for_key" do
      :ok = Index.create_access_key("AKID3", "s", "")
      :ok = Index.upsert_grant("AKID3", "wallpapers", :write)
      :ok = Index.upsert_grant("AKID3", "imouto-images", :read)

      assert [%{access_key_id: "AKID3", permission: :write}] =
               Index.list_bucket_grants("wallpapers")

      assert grants = Index.list_grants_for_key("AKID3")
      assert Enum.map(grants, & &1.bucket) |> Enum.sort() == ["imouto-images", "wallpapers"]
    end

    test "upsert overwrites permission when called again on same (key, bucket)" do
      :ok = Index.create_access_key("AKID4", "s", "")
      :ok = Index.upsert_grant("AKID4", "b", :read)
      :ok = Index.upsert_grant("AKID4", "b", :admin)

      assert [%{permission: :admin}] = Index.list_bucket_grants("b")
    end

    test "delete_grant removes a single (key, bucket) row" do
      :ok = Index.create_access_key("AKID5", "s", "")
      :ok = Index.upsert_grant("AKID5", "a", :read)
      :ok = Index.upsert_grant("AKID5", "b", :read)
      :ok = Index.delete_grant("AKID5", "a")

      assert [%{bucket: "b"}] = Index.list_grants_for_key("AKID5")
    end

    test "effective_grant returns the highest tier across specific + global grants" do
      :ok = Index.create_access_key("AKID6", "s", "")
      assert :none = Index.effective_grant("AKID6", "any")

      :ok = Index.upsert_grant("AKID6", "specific", :read)
      assert :read = Index.effective_grant("AKID6", "specific")
      assert :none = Index.effective_grant("AKID6", "other")

      :ok = Index.upsert_grant("AKID6", "*", :write)
      # global :write fills in for buckets without a specific grant
      assert :write = Index.effective_grant("AKID6", "other")
      # specific :read is dominated by global :write
      assert :write = Index.effective_grant("AKID6", "specific")

      :ok = Index.upsert_grant("AKID6", "specific", :admin)
      # specific :admin now beats the global :write
      assert :admin = Index.effective_grant("AKID6", "specific")
    end

    test "set_bucket_public_read flips the column; bucket_public_read?/1 reads it" do
      :ok = Index.ensure_bucket("public-test")
      refute Index.bucket_public_read?("public-test")

      :ok = Index.set_bucket_public_read("public-test", true)
      assert Index.bucket_public_read?("public-test")

      :ok = Index.set_bucket_public_read("public-test", false)
      refute Index.bucket_public_read?("public-test")
    end

    test "touch_access_key_last_used is async — caller doesn't wait" do
      :ok = Index.create_access_key("AKID7", "s", "")
      :ok = Index.touch_access_key_last_used("AKID7")

      # The cast may race; force a synchronous round-trip to flush the mailbox.
      _ = Index.list_access_keys()

      {:ok, key} = Index.get_access_key("AKID7")
      assert is_integer(key.last_used_at)
    end
  end

  describe "Index round-trip" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-test-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      pid = start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      %{pid: pid, db: db, tmp: tmp}
    end

    test "put / get / delete" do
      :ok = Index.put("b", "k", 42, "etag", "image/png", 1_700_000_000)
      assert {:ok, %{size: 42, etag: "etag"}} = Index.get("b", "k")
      :ok = Index.delete("b", "k")
      assert :not_found == Index.get("b", "k")
    end

    test "backup_to/1 produces a readable snapshot of the index", %{tmp: tmp} do
      :ok = Index.put("b", "k", 5, "etag", nil, 0)

      target = Path.join(tmp, "snapshot.db")
      assert :ok = Index.backup_to(target)
      assert File.regular?(target)

      # Open the snapshot and verify the row landed.
      {:ok, conn} = Exqlite.Sqlite3.open(target)
      {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT key, size FROM objects")
      {:row, [k, sz]} = Exqlite.Sqlite3.step(conn, stmt)
      :ok = Exqlite.Sqlite3.release(conn, stmt)
      :ok = Exqlite.Sqlite3.close(conn)

      assert k == "k"
      assert sz == 5
    end

    test "list with prefix and pagination" do
      for k <- ["a", "b/1", "b/2", "b/3", "c"] do
        :ok = Index.put("b", k, 1, "e", nil, 0)
      end

      {entries, [], false, nil} = Index.list("b", prefix: "b/", max_keys: 10)
      assert Enum.map(entries, & &1.key) == ["b/1", "b/2", "b/3"]

      {first_page, [], true, next} = Index.list("b", prefix: "b/", max_keys: 2)
      assert Enum.map(first_page, & &1.key) == ["b/1", "b/2"]

      {second_page, [], false, nil} =
        Index.list("b", prefix: "b/", max_keys: 2, continuation: next)

      assert Enum.map(second_page, & &1.key) == ["b/3"]
    end

    test "list with delimiter rolls keys into common prefixes" do
      for k <- ["a/1", "a/2", "a/sub/1", "b/1", "top.txt"] do
        :ok = Index.put("d", k, 1, "e", nil, 0)
      end

      {contents, cps, false, nil} = Index.list("d", delimiter: "/", max_keys: 10)
      assert Enum.map(contents, & &1.key) == ["top.txt"]
      assert cps == ["a/", "b/"]
    end

    test "list with prefix + delimiter walks one tier" do
      for k <- ["a/", "a/1", "a/2", "a/sub/1", "a/sub/2"] do
        :ok = Index.put("d", k, 1, "e", nil, 0)
      end

      {contents, cps, false, nil} =
        Index.list("d", prefix: "a/", delimiter: "/", max_keys: 10)

      # Note "a/" itself is a content (key starts with prefix, no delimiter after).
      assert Enum.map(contents, & &1.key) == ["a/", "a/1", "a/2"]
      assert cps == ["a/sub/"]
    end

    test "list pagination across common prefixes uses continuation cursor" do
      for k <- ["a/1", "a/2", "b/1", "b/2", "c/1"] do
        :ok = Index.put("d", k, 1, "e", nil, 0)
      end

      {[], [cp1], true, next1} =
        Index.list("d", delimiter: "/", max_keys: 1)

      assert cp1 == "a/"

      {[], [cp2], true, next2} =
        Index.list("d", delimiter: "/", max_keys: 1, continuation: next1)

      assert cp2 == "b/"

      {[], [cp3], false, nil} =
        Index.list("d", delimiter: "/", max_keys: 1, continuation: next2)

      assert cp3 == "c/"
    end
  end

  describe "S3XML.parse_complete_body/1" do
    test "parses parts in client order" do
      xml = """
      <?xml version="1.0" encoding="UTF-8"?>
      <CompleteMultipartUpload>
        <Part><PartNumber>2</PartNumber><ETag>"bbb"</ETag></Part>
        <Part><PartNumber>1</PartNumber><ETag>"aaa"</ETag></Part>
      </CompleteMultipartUpload>
      """

      assert {:ok, [{2, "\"bbb\""}, {1, "\"aaa\""}]} = S3XML.parse_complete_body(xml)
    end

    test "rejects non-CMU root" do
      assert {:error, :bad_root} = S3XML.parse_complete_body("<Foo/>")
    end

    test "rejects malformed xml" do
      assert {:error, :invalid_xml} = S3XML.parse_complete_body("<Foo><Bar")
    end
  end

  describe "Multipart.multipart_etag/1" do
    test "matches the canonical S3 formula" do
      # Two parts, each MD5 of "hello" (5d41402abc4b2a76b9719d911017c592).
      etag = Multipart.multipart_etag([{1, "5d41402abc4b2a76b9719d911017c592"},
                                       {2, "5d41402abc4b2a76b9719d911017c592"}])

      # md5(decode_hex(e1) || decode_hex(e2)) followed by "-2".
      bin = :binary.decode_hex("5d41402abc4b2a76b9719d911017c592") |> :binary.copy(2)
      expected = :crypto.hash(:md5, bin) |> Base.encode16(case: :lower)
      assert etag == "#{expected}-2"
    end
  end

  describe "Multipart end-to-end" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-mp-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      %{root: tmp}
    end

    test "initiate -> upload parts -> complete reassembles in order", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("b", "blob", "application/octet-stream")

      part1 = :binary.copy(<<0xAA>>, 1024)
      part2 = :binary.copy(<<0xBB>>, 512)

      {conn1, _} = put_part_conn(part1)
      {:ok, _, etag1} = Multipart.upload_part(conn1, root, upload_id, 1)

      {conn2, _} = put_part_conn(part2)
      {:ok, _, etag2} = Multipart.upload_part(conn2, root, upload_id, 2)

      {:ok, %{etag: etag, size: size, bucket: "b", key: "blob"}} =
        Multipart.complete(root, upload_id, [{1, etag1}, {2, etag2}])

      assert size == 1536
      assert etag =~ ~r/^[a-f0-9]{32}-2$/

      blob = Storage.blob_path(root, "b", "blob") |> File.read!()
      assert blob == part1 <> part2
      assert {:ok, %{size: 1536, etag: ^etag}} = Index.get("b", "blob")

      # Upload temp dir cleaned up.
      refute File.exists?(Storage.uploads_dir(root, upload_id))
    end

    test "abort cleans up parts and metadata", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("b", "blob", nil)
      {conn, _} = put_part_conn(:binary.copy(<<1>>, 100))
      {:ok, _, _} = Multipart.upload_part(conn, root, upload_id, 1)

      assert File.exists?(Storage.uploads_dir(root, upload_id))
      assert :ok = Multipart.abort(root, upload_id)
      refute File.exists?(Storage.uploads_dir(root, upload_id))
      assert :not_found = Index.get_upload(upload_id)
    end

    test "complete rejects mismatched etag", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("b", "blob", nil)
      {conn, _} = put_part_conn(<<1, 2, 3>>)
      {:ok, _, _real_etag} = Multipart.upload_part(conn, root, upload_id, 1)

      assert {:error, {:part_mismatch, 1}} =
               Multipart.complete(root, upload_id, [{1, "deadbeef"}])
    end

    test "concat_parts surfaces missing-part with the part number", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("b", "blob", nil)
      {conn, _} = put_part_conn(<<1, 2, 3>>)
      {:ok, _, etag1} = Multipart.upload_part(conn, root, upload_id, 1)

      # Delete part 1 from disk *after* it's been recorded — simulates a race
      # where validate_parts saw the row but concat_parts can't open the file.
      File.rm!(Storage.part_path(root, upload_id, 1))

      assert {:error, {:missing_part, 1}} =
               Storage.concat_parts(root, upload_id, "b", "blob", [{1, etag1}])
    end
  end

  defp put_part_conn(body) do
    conn = Plug.Test.conn(:put, "/x", body)
    {conn, body}
  end

  describe "Storage.stream_put — aws-chunked unwrap" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-chunked-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)
      %{root: tmp}
    end

    test "unsigned chunked body with CRC32 trailer round-trips clean", %{root: root} do
      data = :crypto.strong_rand_bytes(50_000)
      body = chunked(data, trailer: "x-amz-checksum-crc32:abcdef12")
      conn = chunked_conn(body, "STREAMING-UNSIGNED-PAYLOAD-TRAILER")

      {:ok, _conn, size, etag} = Storage.stream_put(conn, root, "b", "k")

      assert size == byte_size(data)
      assert etag == :crypto.hash(:md5, data) |> Base.encode16(case: :lower)
      assert File.read!(Storage.blob_path(root, "b", "k")) == data
    end

    test "signed chunked body with chunk-signature extension is unwrapped", %{root: root} do
      data = :crypto.strong_rand_bytes(20_000)
      body = chunked(data, ext: ";chunk-signature=" <> String.duplicate("a", 64))
      conn = chunked_conn(body, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")

      {:ok, _conn, size, _etag} = Storage.stream_put(conn, root, "b", "k")
      assert size == byte_size(data)
      assert File.read!(Storage.blob_path(root, "b", "k")) == data
    end

    test "multi-chunk body is glued back together in order", %{root: root} do
      data = :crypto.strong_rand_bytes(30_000)
      body = chunked(data, chunk_size: 4_096)
      conn = chunked_conn(body, "STREAMING-UNSIGNED-PAYLOAD-TRAILER")

      {:ok, _conn, size, etag} = Storage.stream_put(conn, root, "b", "k")
      assert size == byte_size(data)
      assert etag == :crypto.hash(:md5, data) |> Base.encode16(case: :lower)
      assert File.read!(Storage.blob_path(root, "b", "k")) == data
    end

    test "Content-Encoding: aws-chunked also activates the unwrap path", %{root: root} do
      data = "hello aws-chunked world"
      body = chunked(data)

      conn =
        Plug.Test.conn(:put, "/b/k", body)
        |> Plug.Conn.put_req_header("content-encoding", "aws-chunked")

      {:ok, _conn, size, _etag} = Storage.stream_put(conn, root, "b", "k")
      assert size == byte_size(data)
      assert File.read!(Storage.blob_path(root, "b", "k")) == data
    end

    test "plain bodies still pass through unchanged", %{root: root} do
      data = "no chunked envelope here"
      conn = Plug.Test.conn(:put, "/b/k", data)

      {:ok, _conn, size, etag} = Storage.stream_put(conn, root, "b", "k")
      assert size == byte_size(data)
      assert etag == :crypto.hash(:md5, data) |> Base.encode16(case: :lower)
      assert File.read!(Storage.blob_path(root, "b", "k")) == data
    end
  end

  defp chunked_conn(body, sha256_marker) do
    Plug.Test.conn(:put, "/b/k", body)
    |> Plug.Conn.put_req_header("x-amz-content-sha256", sha256_marker)
  end

  defp chunked(data, opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, max(byte_size(data), 1))
    ext = Keyword.get(opts, :ext, "")
    trailer = Keyword.get(opts, :trailer, "")

    chunks = split_for_test(data, chunk_size)

    framed =
      Enum.map_join(chunks, "", fn c ->
        hex = byte_size(c) |> Integer.to_string(16)
        "#{hex}#{ext}\r\n" <> c <> "\r\n"
      end)

    trailer_line = if trailer == "", do: "", else: trailer <> "\r\n"
    framed <> "0#{ext}\r\n" <> trailer_line <> "\r\n"
  end

  defp split_for_test("", _n), do: []
  defp split_for_test(data, n) when byte_size(data) <= n, do: [data]

  defp split_for_test(data, n) do
    <<head::binary-size(n), rest::binary>> = data
    [head | split_for_test(rest, n)]
  end

  describe "GC sweep" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-gc-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})

      start_supervised!(
        {GC, root: tmp, interval_ms: 0, abandon_after_seconds: 60, blob_grace_seconds: 0}
      )

      on_exit(fn -> File.rm_rf!(tmp) end)
      %{root: tmp}
    end

    test "removes uploads older than abandon_after", %{root: root} do
      {:ok, fresh_id} = Multipart.initiate("b", "fresh", nil)
      {:ok, stale_id} = Multipart.initiate("b", "stale", nil)

      {conn, _} = put_part_conn(<<1, 2, 3>>)
      {:ok, _, _} = Multipart.upload_part(conn, root, stale_id, 1)

      # Backdate the stale upload past the cutoff.
      :ok = backdate_upload(stale_id, System.system_time(:second) - 600)

      assert %{abandoned: 1, orphans: 0} = GC.run_now()

      assert :not_found = Index.get_upload(stale_id)
      assert {:ok, _} = Index.get_upload(fresh_id)
      refute File.exists?(Storage.uploads_dir(root, stale_id))
    end

    test "removes orphan part dirs with no DB row", %{root: root} do
      orphan = "orphan-id"
      File.mkdir_p!(Storage.uploads_dir(root, orphan))
      File.write!(Path.join(Storage.uploads_dir(root, orphan), "1"), "junk")

      assert %{orphans: 1} = GC.run_now()
      refute File.exists?(Storage.uploads_dir(root, orphan))
    end

    test "removes blobs whose objects row is missing", %{root: root} do
      # Real PUT (blob + index entry).
      conn = Plug.Test.conn(:put, "/x", "hi")
      {:ok, _, sz, etag} = Storage.stream_put(conn, root, "imouto", "kept")
      :ok = Index.put("imouto", "kept", sz, etag, nil, 0)

      # Crashed PUT: blob on disk, no index row.
      orphan_path = Storage.blob_path(root, "imouto", "lost")
      File.mkdir_p!(Path.dirname(orphan_path))
      File.write!(orphan_path, "orphan body")

      # Crashed PUT mid-write: tmp file leftover.
      tmp_path = orphan_path <> ".tmp.deadbeef"
      File.write!(tmp_path, "half-written")

      assert %{orphan_blobs: 2} = GC.run_now()

      assert File.exists?(Storage.blob_path(root, "imouto", "kept"))
      refute File.exists?(orphan_path)
      refute File.exists?(tmp_path)
    end

    test "respects blob grace window", %{root: _root} do
      # Re-supervise GC with a long grace; new blobs should NOT be swept.
      stop_supervised!(GC)
      tmp = Application.fetch_env!(:kafun, :root)

      start_supervised!(
        {GC, root: tmp, interval_ms: 0, abandon_after_seconds: 60, blob_grace_seconds: 3600}
      )

      orphan_path = Storage.blob_path(tmp, "imouto", "fresh-orphan")
      File.mkdir_p!(Path.dirname(orphan_path))
      File.write!(orphan_path, "fresh")

      assert %{orphan_blobs: 0} = GC.run_now()
      assert File.exists?(orphan_path)
    end
  end

  defp backdate_upload(upload_id, ts) do
    {:ok, conn} = Exqlite.Sqlite3.open(Application.fetch_env!(:kafun, :root) |> Path.join("index.db"))
    :ok = Exqlite.Sqlite3.execute(conn, "UPDATE uploads SET initiated_at = #{ts} WHERE upload_id = '#{upload_id}'")
    :ok = Exqlite.Sqlite3.close(conn)
  end

  describe "Telemetry events" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-tel-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      start_supervised!({GC, root: tmp, interval_ms: 0, abandon_after_seconds: 60, blob_grace_seconds: 0})

      handler = make_ref()
      test_pid = self()

      :telemetry.attach_many(
        handler,
        [
          [:kafun, :put, :stop],
          [:kafun, :get, :stop],
          [:kafun, :multipart, :complete],
          [:kafun, :gc, :run]
        ],
        fn name, measurements, metadata, _ ->
          send(test_pid, {:telemetry, name, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(handler)
        File.rm_rf!(tmp)
      end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      %{root: tmp}
    end

    test "PUT through the router emits put.stop" do
      conn =
        Plug.Test.conn(:put, "/imouto/k", "hello")
        |> Plug.Conn.put_req_header("content-type", "text/plain")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert_receive {:telemetry, [:kafun, :put, :stop],
                      %{size: 5, duration: _}, %{bucket: "imouto", key: "k"}}
    end

    test "GC.run_now emits gc.run with both counters" do
      GC.run_now()

      assert_receive {:telemetry, [:kafun, :gc, :run],
                      %{abandoned_uploads: _, orphan_dirs: _, duration: _}, _},
                     1_000
    end

    test "multipart complete emits multipart.complete with parts count", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("imouto", "blob", nil)
      {conn, _} = put_part_conn(<<1, 2, 3, 4>>)
      {:ok, _, etag1} = Multipart.upload_part(conn, root, upload_id, 1)
      {conn2, _} = put_part_conn(<<5, 6, 7, 8>>)
      {:ok, _, etag2} = Multipart.upload_part(conn2, root, upload_id, 2)

      complete_xml = """
      <CompleteMultipartUpload>
        <Part><PartNumber>1</PartNumber><ETag>"#{etag1}"</ETag></Part>
        <Part><PartNumber>2</PartNumber><ETag>"#{etag2}"</ETag></Part>
      </CompleteMultipartUpload>
      """

      conn =
        Plug.Test.conn(:post, "/imouto/blob?uploadId=#{upload_id}", complete_xml)
        |> Plug.Conn.put_req_header("content-type", "application/xml")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200

      assert_receive {:telemetry, [:kafun, :multipart, :complete],
                      %{size: 8, parts: 2, duration: _},
                      %{bucket: "imouto", key: "blob", upload_id: ^upload_id}}
    end
  end

  describe "Bucket sub-resource stubs" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-stub-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      :ok
    end

    test "?location returns LocationConstraint with the kafun region" do
      conn =
        Plug.Test.conn(:get, "/imouto?location") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<LocationConstraint"
      assert conn.resp_body =~ "us-east-1"
    end

    test "?acl returns a stub AccessControlPolicy with FULL_CONTROL" do
      conn =
        Plug.Test.conn(:get, "/imouto?acl") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<AccessControlPolicy"
      assert conn.resp_body =~ "<Permission>FULL_CONTROL</Permission>"
    end

    test "?versioning returns an empty VersioningConfiguration" do
      conn =
        Plug.Test.conn(:get, "/imouto?versioning") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<VersioningConfiguration"
    end

    test "?policy returns 404 NoSuchBucketPolicy" do
      conn = Plug.Test.conn(:get, "/imouto?policy") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchBucketPolicy"
    end

    test "?cors returns 404 NoSuchCORSConfiguration" do
      conn = Plug.Test.conn(:get, "/imouto?cors") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchCORSConfiguration"
    end

    test "?lifecycle returns 404 NoSuchLifecycleConfiguration" do
      conn =
        Plug.Test.conn(:get, "/imouto?lifecycle") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchLifecycleConfiguration"
    end

    test "?tagging returns 404 NoSuchTagSet" do
      conn = Plug.Test.conn(:get, "/imouto?tagging") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchTagSet"
    end

    test "stub queries on a missing bucket still return NoSuchBucket" do
      conn =
        Plug.Test.conn(:get, "/ghost?location") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchBucket"
    end
  end

  describe "Router DeleteBucket" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-delbucket-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      %{root: tmp}
    end

    test "204 on a created, empty bucket; bucket no longer exists afterwards", %{root: root} do
      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      assert Index.bucket_exists?("imouto")

      conn = Plug.Test.conn(:delete, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 204
      assert conn.resp_body == ""

      refute Index.bucket_exists?("imouto")
      refute File.exists?(Path.join(root, "imouto"))
    end

    test "404 NoSuchBucket on a never-created bucket" do
      conn = Plug.Test.conn(:delete, "/never") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchBucket"
    end

    test "409 BucketNotEmpty when objects remain" do
      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      Plug.Test.conn(:put, "/imouto/k", "x") |> Kafun.Router.call(Kafun.Router.init([]))

      conn = Plug.Test.conn(:delete, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 409
      assert conn.resp_body =~ "BucketNotEmpty"
      assert Index.bucket_exists?("imouto")
    end

    test "delete-then-recreate works (the bucket dir gets re-created on PUT)" do
      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      Plug.Test.conn(:delete, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      recreate =
        Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      assert recreate.status == 200
      assert Index.bucket_exists?("imouto")
    end
  end

  describe "Conditional headers" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-cond-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      put_conn =
        Plug.Test.conn(:put, "/imouto/k", "hello")
        |> Kafun.Router.call(Kafun.Router.init([]))

      [quoted_etag] = Plug.Conn.get_resp_header(put_conn, "etag")
      etag = String.trim(quoted_etag, ~s|"|)

      %{etag: etag}
    end

    test "GET If-None-Match matching the current etag returns 304", %{etag: etag} do
      conn =
        Plug.Test.conn(:get, "/imouto/k")
        |> Plug.Conn.put_req_header("if-none-match", ~s|"#{etag}"|)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 304
      assert conn.resp_body == ""
      assert [_etag] = Plug.Conn.get_resp_header(conn, "etag")
    end

    test "GET If-None-Match: * always returns 304 for an existing object" do
      conn =
        Plug.Test.conn(:get, "/imouto/k")
        |> Plug.Conn.put_req_header("if-none-match", "*")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 304
    end

    test "GET If-Match with a stale etag returns 412 PreconditionFailed" do
      conn =
        Plug.Test.conn(:get, "/imouto/k")
        |> Plug.Conn.put_req_header("if-match", ~s|"deadbeef"|)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 412
      assert conn.resp_body =~ "PreconditionFailed"
    end

    test "GET If-Modified-Since in the future returns 304" do
      future = DateTime.utc_now() |> DateTime.add(3600, :second)
      hdr = Calendar.strftime(future, "%a, %d %b %Y %H:%M:%S GMT")

      conn =
        Plug.Test.conn(:get, "/imouto/k")
        |> Plug.Conn.put_req_header("if-modified-since", hdr)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 304
    end

    test "GET If-Unmodified-Since in the past returns 412" do
      past = DateTime.utc_now() |> DateTime.add(-3600, :second)
      hdr = Calendar.strftime(past, "%a, %d %b %Y %H:%M:%S GMT")

      conn =
        Plug.Test.conn(:get, "/imouto/k")
        |> Plug.Conn.put_req_header("if-unmodified-since", hdr)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 412
    end

    test "PUT If-None-Match: * fails with 412 when the key already exists" do
      conn =
        Plug.Test.conn(:put, "/imouto/k", "would overwrite")
        |> Plug.Conn.put_req_header("if-none-match", "*")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 412

      # Confirm the existing object was not overwritten.
      get = Plug.Test.conn(:get, "/imouto/k") |> Kafun.Router.call(Kafun.Router.init([]))
      assert get.resp_body == "hello"
    end

    test "PUT If-None-Match: * succeeds for a brand-new key" do
      conn =
        Plug.Test.conn(:put, "/imouto/fresh", "new")
        |> Plug.Conn.put_req_header("if-none-match", "*")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "PUT If-Match: * fails with 412 when the key does not exist" do
      conn =
        Plug.Test.conn(:put, "/imouto/missing", "x")
        |> Plug.Conn.put_req_header("if-match", "*")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 412
    end

    test "CopyObject x-amz-copy-source-if-match with matching etag succeeds", %{etag: etag} do
      conn =
        Plug.Test.conn(:put, "/imouto/dst", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/k")
        |> Plug.Conn.put_req_header("x-amz-copy-source-if-match", ~s|"#{etag}"|)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
    end

    test "CopyObject x-amz-copy-source-if-match mismatching returns 412" do
      conn =
        Plug.Test.conn(:put, "/imouto/dst", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/k")
        |> Plug.Conn.put_req_header("x-amz-copy-source-if-match", ~s|"deadbeef"|)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 412
    end

    test "CopyObject x-amz-copy-source-if-none-match matching returns 412", %{etag: etag} do
      conn =
        Plug.Test.conn(:put, "/imouto/dst", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/k")
        |> Plug.Conn.put_req_header("x-amz-copy-source-if-none-match", ~s|"#{etag}"|)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 412
    end
  end

  describe "User metadata round-trip" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-meta-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      :ok
    end

    test "PUT preserves x-amz-meta-* headers, GET and HEAD echo them back" do
      Plug.Test.conn(:put, "/imouto/key", "body")
      |> Plug.Conn.put_req_header("x-amz-meta-author", "naka")
      |> Plug.Conn.put_req_header("x-amz-meta-source", "kafun-test")
      |> Kafun.Router.call(Kafun.Router.init([]))

      get_conn = Plug.Test.conn(:get, "/imouto/key") |> Kafun.Router.call(Kafun.Router.init([]))
      assert get_conn.status == 200
      assert ["naka"] = Plug.Conn.get_resp_header(get_conn, "x-amz-meta-author")
      assert ["kafun-test"] = Plug.Conn.get_resp_header(get_conn, "x-amz-meta-source")

      head_conn = Plug.Test.conn(:head, "/imouto/key") |> Kafun.Router.call(Kafun.Router.init([]))
      assert head_conn.status == 200
      assert ["naka"] = Plug.Conn.get_resp_header(head_conn, "x-amz-meta-author")
    end

    test "PUT without metadata does not emit x-amz-meta-* headers on GET" do
      Plug.Test.conn(:put, "/imouto/k", "body") |> Kafun.Router.call(Kafun.Router.init([]))

      conn = Plug.Test.conn(:get, "/imouto/k") |> Kafun.Router.call(Kafun.Router.init([]))
      assert Enum.all?(conn.resp_headers, fn {k, _} -> not String.starts_with?(k, "x-amz-meta-") end)
    end

    test "CopyObject metadata-directive=REPLACE drops source meta and applies request headers" do
      Plug.Test.conn(:put, "/imouto/src", "src body")
      |> Plug.Conn.put_req_header("x-amz-meta-old", "should-be-gone")
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> Kafun.Router.call(Kafun.Router.init([]))

      Plug.Test.conn(:put, "/imouto/dst", "")
      |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/src")
      |> Plug.Conn.put_req_header("x-amz-metadata-directive", "REPLACE")
      |> Plug.Conn.put_req_header("x-amz-meta-fresh", "yes")
      |> Plug.Conn.put_req_header("content-type", "image/png")
      |> Kafun.Router.call(Kafun.Router.init([]))

      conn = Plug.Test.conn(:head, "/imouto/dst") |> Kafun.Router.call(Kafun.Router.init([]))
      assert ["yes"] = Plug.Conn.get_resp_header(conn, "x-amz-meta-fresh")
      assert [] = Plug.Conn.get_resp_header(conn, "x-amz-meta-old")
      assert ["image/png"] = Plug.Conn.get_resp_header(conn, "content-type")
    end

    test "CopyObject (default COPY) preserves source metadata on the destination" do
      Plug.Test.conn(:put, "/imouto/src", "src body")
      |> Plug.Conn.put_req_header("x-amz-meta-tag", "carry-me")
      |> Kafun.Router.call(Kafun.Router.init([]))

      Plug.Test.conn(:put, "/imouto/dst", "")
      |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/src")
      |> Kafun.Router.call(Kafun.Router.init([]))

      conn = Plug.Test.conn(:head, "/imouto/dst") |> Kafun.Router.call(Kafun.Router.init([]))
      assert ["carry-me"] = Plug.Conn.get_resp_header(conn, "x-amz-meta-tag")
    end

    test "metadata supplied at multipart Initiate is applied on Complete" do
      init =
        Plug.Test.conn(:post, "/imouto/big?uploads")
        |> Plug.Conn.put_req_header("x-amz-meta-flavour", "vanilla")
        |> Kafun.Router.call(Kafun.Router.init([]))

      [_, upload_id] =
        Regex.run(~r{<UploadId>(.+?)</UploadId>}, init.resp_body)

      part_conn =
        Plug.Test.conn(:put, "/imouto/big?partNumber=1&uploadId=#{upload_id}", "hello mp")
        |> Kafun.Router.call(Kafun.Router.init([]))

      [quoted] = Plug.Conn.get_resp_header(part_conn, "etag")
      part_etag = String.trim(quoted, ~s|"|)

      complete_xml =
        ~s|<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"#{part_etag}"</ETag></Part></CompleteMultipartUpload>|

      Plug.Test.conn(:post, "/imouto/big?uploadId=#{upload_id}", complete_xml)
      |> Kafun.Router.call(Kafun.Router.init([]))

      head = Plug.Test.conn(:head, "/imouto/big") |> Kafun.Router.call(Kafun.Router.init([]))
      assert ["vanilla"] = Plug.Conn.get_resp_header(head, "x-amz-meta-flavour")
    end
  end

  describe "Router CopyObject + UploadPartCopy" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-copy-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      Plug.Test.conn(:put, "/imouto/src/key", "hello copy world")
      |> Plug.Conn.put_req_header("content-type", "text/plain")
      |> Kafun.Router.call(Kafun.Router.init([]))

      %{root: tmp}
    end

    test "CopyObject duplicates bytes, preserves etag and content-type", %{root: root} do
      conn =
        Plug.Test.conn(:put, "/imouto/dst/key", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/src/key")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<CopyObjectResult"
      assert conn.resp_body =~ "<ETag>"

      {:ok, %{size: size, etag: etag, content_type: ct}} = Index.get("imouto", "dst/key")
      assert size == byte_size("hello copy world")
      assert etag == :crypto.hash(:md5, "hello copy world") |> Base.encode16(case: :lower)
      assert ct == "text/plain"

      assert File.read!(Storage.blob_path(root, "imouto", "dst/key")) == "hello copy world"
    end

    test "CopyObject returns NoSuchKey when source is missing" do
      conn =
        Plug.Test.conn(:put, "/imouto/dst/key", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/never-existed")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchKey"
    end

    test "CopyObject decodes URL-encoded source keys", %{root: root} do
      Plug.Test.conn(:put, "/imouto/funny key/with spaces", "encoded body")
      |> Kafun.Router.call(Kafun.Router.init([]))

      conn =
        Plug.Test.conn(:put, "/imouto/dst", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/funny%20key/with%20spaces")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert File.read!(Storage.blob_path(root, "imouto", "dst")) == "encoded body"
    end

    test "CopyObject accepts source without leading slash", %{root: root} do
      conn =
        Plug.Test.conn(:put, "/imouto/dst", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "imouto/src/key")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert File.read!(Storage.blob_path(root, "imouto", "dst")) == "hello copy world"
    end

    test "CopyObject with malformed copy-source returns InvalidArgument" do
      conn =
        Plug.Test.conn(:put, "/imouto/dst", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "no-slash-no-key")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 400
      assert conn.resp_body =~ "InvalidArgument"
    end

    test "UploadPartCopy ingests a full-source copy as a multipart part", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("imouto", "assembled", nil)

      conn =
        Plug.Test.conn(:put, "/imouto/assembled?partNumber=1&uploadId=#{upload_id}", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/src/key")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<CopyPartResult"

      part_path = Storage.part_path(root, upload_id, 1)
      assert File.read!(part_path) == "hello copy world"

      [part] = Index.list_parts(upload_id)
      assert part.size == byte_size("hello copy world")
      assert part.etag == :crypto.hash(:md5, "hello copy world") |> Base.encode16(case: :lower)
    end

    test "UploadPartCopy honours x-amz-copy-source-range", %{root: root} do
      {:ok, upload_id} = Multipart.initiate("imouto", "assembled", nil)

      # bytes 6..9 of "hello copy world" = "copy"
      conn =
        Plug.Test.conn(:put, "/imouto/assembled?partNumber=1&uploadId=#{upload_id}", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/src/key")
        |> Plug.Conn.put_req_header("x-amz-copy-source-range", "bytes=6-9")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert File.read!(Storage.part_path(root, upload_id, 1)) == "copy"
    end

    test "UploadPartCopy returns NoSuchUpload for an unknown upload id" do
      conn =
        Plug.Test.conn(:put, "/imouto/x?partNumber=1&uploadId=ghost", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/src/key")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchUpload"
    end

    test "UploadPartCopy returns NoSuchKey when source is missing" do
      {:ok, upload_id} = Multipart.initiate("imouto", "x", nil)

      conn =
        Plug.Test.conn(:put, "/imouto/x?partNumber=1&uploadId=#{upload_id}", "")
        |> Plug.Conn.put_req_header("x-amz-copy-source", "/imouto/never-existed")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchKey"
    end
  end

  describe "S3XML.parse_delete_body/1" do
    test "extracts keys in document order with quiet flag false by default" do
      xml = """
      <Delete>
        <Object><Key>a/1</Key></Object>
        <Object><Key>a/2</Key></Object>
        <Object><Key>b</Key></Object>
      </Delete>
      """

      assert {:ok, %{keys: ["a/1", "a/2", "b"], quiet: false}} =
               S3XML.parse_delete_body(xml)
    end

    test "honours <Quiet>true</Quiet>" do
      xml = "<Delete><Object><Key>k</Key></Object><Quiet>true</Quiet></Delete>"
      assert {:ok, %{keys: ["k"], quiet: true}} = S3XML.parse_delete_body(xml)
    end

    test "drops <Object> entries without a Key" do
      xml = "<Delete><Object/><Object><Key>k</Key></Object></Delete>"
      assert {:ok, %{keys: ["k"], quiet: false}} = S3XML.parse_delete_body(xml)
    end

    test "rejects non-Delete root and malformed XML" do
      assert {:error, :bad_root} = S3XML.parse_delete_body("<Other/>")
      assert {:error, :invalid_xml} = S3XML.parse_delete_body("<broken>")
    end
  end

  describe "S3XML.delete_result/3" do
    test "emits Deleted and Error blocks in non-quiet mode" do
      out =
        S3XML.delete_result(["a", "b"], [{"bad", "InvalidKey", "key is not valid"}], false)
        |> IO.iodata_to_binary()

      assert out =~ "<Deleted><Key>a</Key></Deleted>"
      assert out =~ "<Deleted><Key>b</Key></Deleted>"
      assert out =~ "<Error><Key>bad</Key><Code>InvalidKey</Code>"
    end

    test "suppresses Deleted blocks when quiet, keeps Error blocks" do
      out =
        S3XML.delete_result(["a"], [{"bad", "InvalidKey", "x"}], true)
        |> IO.iodata_to_binary()

      refute out =~ "<Deleted>"
      assert out =~ "<Error><Key>bad</Key>"
    end
  end

  describe "Router DeleteObjects" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-deleteobjs-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      for k <- ["a/1", "a/2", "b/1"] do
        Plug.Test.conn(:put, "/imouto/#{k}", "body of #{k}")
        |> Kafun.Router.call(Kafun.Router.init([]))
      end

      %{root: tmp}
    end

    test "deletes the listed keys, leaves others intact, returns Deleted XML", %{root: root} do
      body = """
      <Delete>
        <Object><Key>a/1</Key></Object>
        <Object><Key>a/2</Key></Object>
      </Delete>
      """

      conn =
        Plug.Test.conn(:post, "/imouto?delete", body)
        |> Plug.Conn.put_req_header("content-type", "application/xml")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<Deleted><Key>a/1</Key></Deleted>"
      assert conn.resp_body =~ "<Deleted><Key>a/2</Key></Deleted>"
      refute conn.resp_body =~ "<Error>"

      assert :not_found = Index.get("imouto", "a/1")
      assert :not_found = Index.get("imouto", "a/2")
      assert {:ok, _} = Index.get("imouto", "b/1")

      refute File.exists?(Storage.blob_path(root, "imouto", "a/1"))
      refute File.exists?(Storage.blob_path(root, "imouto", "a/2"))
      assert File.exists?(Storage.blob_path(root, "imouto", "b/1"))
    end

    test "is idempotent: deleting a key that does not exist is not an error" do
      body = "<Delete><Object><Key>never-existed</Key></Object></Delete>"

      conn =
        Plug.Test.conn(:post, "/imouto?delete", body)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<Deleted><Key>never-existed</Key></Deleted>"
      refute conn.resp_body =~ "<Error>"
    end

    test "invalid keys come back as Error, others still delete", %{root: root} do
      body = """
      <Delete>
        <Object><Key>a/1</Key></Object>
        <Object><Key>../escape</Key></Object>
      </Delete>
      """

      conn =
        Plug.Test.conn(:post, "/imouto?delete", body)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<Deleted><Key>a/1</Key></Deleted>"
      assert conn.resp_body =~ "<Error><Key>../escape</Key><Code>InvalidKey</Code>"

      refute File.exists?(Storage.blob_path(root, "imouto", "a/1"))
    end

    test "quiet mode suppresses Deleted entries but keeps Errors" do
      body = """
      <Delete>
        <Quiet>true</Quiet>
        <Object><Key>a/1</Key></Object>
        <Object><Key>../bad</Key></Object>
      </Delete>
      """

      conn =
        Plug.Test.conn(:post, "/imouto?delete", body)
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      refute conn.resp_body =~ "<Deleted>"
      assert conn.resp_body =~ "<Error><Key>../bad</Key>"
    end

    test "empty Delete body is rejected as MalformedXML" do
      conn =
        Plug.Test.conn(:post, "/imouto?delete", "<Delete></Delete>")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 400
      assert conn.resp_body =~ "MalformedXML"
    end

    test "POST without ?delete is a 400 InvalidRequest" do
      conn =
        Plug.Test.conn(:post, "/imouto", "<Delete/>")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 400
      assert conn.resp_body =~ "InvalidRequest"
    end
  end

  describe "ListObjectsV2 — encoding-type, fetch-owner, cursor precedence" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-list-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      for k <- ["plain", "with space/01", "with space/02", "ünicode/é"] do
        :ok = Index.put("imouto", k, 1, "deadbeef", nil, 1_700_000_000)
      end

      :ok
    end

    test "encoding-type=url echoes the marker and URL-encodes keys" do
      conn =
        Plug.Test.conn(:get, "/imouto?list-type=2&encoding-type=url")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 200
      assert conn.resp_body =~ "<EncodingType>url</EncodingType>"
      assert conn.resp_body =~ "<Key>with%20space/01</Key>"
      assert conn.resp_body =~ "<Key>%C3%BCnicode/%C3%A9</Key>"
      assert conn.resp_body =~ "<Key>plain</Key>"
    end

    test "no encoding-type leaves keys as raw XML-escaped strings" do
      conn =
        Plug.Test.conn(:get, "/imouto?list-type=2") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.resp_body =~ "<Key>with space/01</Key>"
      refute conn.resp_body =~ "<EncodingType>"
    end

    test "fetch-owner=true emits Owner blocks on Contents" do
      conn =
        Plug.Test.conn(:get, "/imouto?list-type=2&fetch-owner=true")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.resp_body =~ "<Owner><ID>kafun</ID><DisplayName>kafun</DisplayName></Owner>"
    end

    test "fetch-owner default omits Owner blocks" do
      conn =
        Plug.Test.conn(:get, "/imouto?list-type=2") |> Kafun.Router.call(Kafun.Router.init([]))

      refute conn.resp_body =~ "<Owner>"
    end

    test "continuation-token wins when both it and start-after are sent" do
      # First page with max-keys=1 → next continuation token points past 'plain'.
      conn1 =
        Plug.Test.conn(:get, "/imouto?list-type=2&max-keys=1")
        |> Kafun.Router.call(Kafun.Router.init([]))

      [_, token] = Regex.run(~r{<NextContinuationToken>(.+?)</NextContinuationToken>}, conn1.resp_body)

      # Send both: a CT past 'plain' and a start-after at the very end. CT must win.
      conn2 =
        Plug.Test.conn(
          :get,
          "/imouto?list-type=2&continuation-token=#{token}&start-after=zzz"
        )
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn2.status == 200
      # Should still return entries past the CT — start-after=zzz would have skipped everything.
      assert conn2.resp_body =~ "<Key>"
    end
  end

  describe "Request id plumbing" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-rid-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      :ok
    end

    test "every response carries x-amz-request-id" do
      conn = Plug.Test.conn(:get, "/healthz") |> Kafun.Router.call(Kafun.Router.init([]))

      assert [id] = Plug.Conn.get_resp_header(conn, "x-amz-request-id")
      assert String.length(id) == 16
      assert Regex.match?(~r/^[0-9A-F]{16}$/, id)
    end

    test "error responses echo the same request id in the body" do
      conn = Plug.Test.conn(:get, "/ghost?list-type=2") |> Kafun.Router.call(Kafun.Router.init([]))

      assert [id] = Plug.Conn.get_resp_header(conn, "x-amz-request-id")
      assert conn.resp_body =~ "<RequestId>#{id}</RequestId>"
      assert conn.resp_body =~ "<HostId>#{id}</HostId>"
    end
  end

  describe "Router NoSuchBucket" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-nsb-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      %{root: tmp}
    end

    test "PUT to a bucket that was never created returns 404 NoSuchBucket" do
      conn =
        Plug.Test.conn(:put, "/ghost/key", "x")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchBucket"
    end

    test "GET listing on a never-created bucket returns 404 NoSuchBucket" do
      conn =
        Plug.Test.conn(:get, "/ghost?list-type=2")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchBucket"
    end

    test "POST ?delete on a never-created bucket returns 404 NoSuchBucket" do
      conn =
        Plug.Test.conn(:post, "/ghost?delete", "<Delete><Object><Key>x</Key></Object></Delete>")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body =~ "NoSuchBucket"
    end
  end

  describe "Router HEAD bucket" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-head-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)
      %{root: tmp}
    end

    test "200 for an existing bucket" do
      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))

      conn = Plug.Test.conn(:head, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 200
      assert conn.resp_body == ""
    end

    test "404 for a bucket that has not been created" do
      conn =
        Plug.Test.conn(:head, "/does-not-exist") |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body == ""
      assert ["NoSuchBucket"] = Plug.Conn.get_resp_header(conn, "x-amz-error-code")
    end

    test "400 for a syntactically invalid bucket name" do
      conn = Plug.Test.conn(:head, "/INVALID") |> Kafun.Router.call(Kafun.Router.init([]))
      assert conn.status == 400
      assert conn.resp_body == ""
      assert ["InvalidBucketName"] = Plug.Conn.get_resp_header(conn, "x-amz-error-code")
    end
  end

  describe "HEAD object 404 surfaces x-amz-error-code" do
    setup do
      tmp = Path.join(System.tmp_dir!(), "kafun-head404-#{System.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      db = Path.join(tmp, "index.db")
      Application.put_env(:kafun, :root, tmp)
      start_supervised!({Index, db_path: db})
      on_exit(fn -> File.rm_rf!(tmp) end)

      Plug.Test.conn(:put, "/imouto") |> Kafun.Router.call(Kafun.Router.init([]))
      :ok
    end

    test "missing key returns empty body with NoSuchKey header" do
      conn =
        Plug.Test.conn(:head, "/imouto/never-existed")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body == ""
      assert ["NoSuchKey"] = Plug.Conn.get_resp_header(conn, "x-amz-error-code")
    end

    test "HEAD on a missing bucket returns NoSuchBucket header (no body)" do
      conn =
        Plug.Test.conn(:head, "/ghost-bucket/some-key")
        |> Kafun.Router.call(Kafun.Router.init([]))

      assert conn.status == 404
      assert conn.resp_body == ""
      assert ["NoSuchBucket"] = Plug.Conn.get_resp_header(conn, "x-amz-error-code")
    end
  end
end
