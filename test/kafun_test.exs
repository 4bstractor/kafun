defmodule KafunTest do
  use ExUnit.Case, async: false

  alias Kafun.{GC, Index, Multipart, Storage, S3XML}

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
  end

  defp put_part_conn(body) do
    conn = Plug.Test.conn(:put, "/x", body)
    {conn, body}
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
end
