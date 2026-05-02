defmodule KafunTest do
  use ExUnit.Case, async: false

  alias Kafun.{Index, Storage, S3XML}

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
          1000,
          [%{key: "a", size: 1, etag: "x", mtime: 0}],
          true,
          "a"
        )
        |> IO.iodata_to_binary()

      assert xml =~ "<IsTruncated>true</IsTruncated>"
      assert xml =~ "<NextContinuationToken>"
      assert xml =~ "<ETag>\"x\"</ETag>"
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

      {entries, false, nil} = Index.list("b", prefix: "b/", max_keys: 10)
      assert Enum.map(entries, & &1.key) == ["b/1", "b/2", "b/3"]

      {first_page, true, next} = Index.list("b", prefix: "b/", max_keys: 2)
      assert Enum.map(first_page, & &1.key) == ["b/1", "b/2"]
      assert next == "b/2"

      {second_page, false, nil} =
        Index.list("b", prefix: "b/", max_keys: 2, start_after: next)

      assert Enum.map(second_page, & &1.key) == ["b/3"]
    end
  end
end
