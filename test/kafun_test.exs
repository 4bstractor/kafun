defmodule KafunTest do
  use ExUnit.Case, async: false

  alias Kafun.{GC, Index, Multipart, Storage, S3XML}

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
