defmodule KafunAdminLiveTest do
  # First LiveView test scaffolding for the admin UI. The endpoint runs with
  # `server: false` in test, so no port is bound — Phoenix.LiveViewTest talks
  # to it in-process. Each test gets its own Index + storage root.
  use ExUnit.Case, async: false

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias Kafun.Index

  @endpoint Kafun.Admin.Endpoint

  setup do
    tmp = Path.join(System.tmp_dir!(), "kafun-lv-#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    db = Path.join(tmp, "index.db")

    previous_root = Application.get_env(:kafun, :root)
    Application.put_env(:kafun, :root, tmp)

    start_supervised!({Phoenix.PubSub, name: Kafun.PubSub})
    start_supervised!({Index, db_path: db})
    start_supervised!(Kafun.Admin.Endpoint)

    :ok = Index.ensure_bucket("upbucket")
    File.mkdir_p!(Path.join(tmp, "upbucket"))

    on_exit(fn ->
      Application.put_env(:kafun, :root, previous_root)
      Application.delete_env(:kafun, :admin_max_upload_files)
      File.rm_rf!(tmp)
    end)

    :ok
  end

  defp entries(n) do
    for i <- 1..n do
      %{
        name: "file-#{i}.bin",
        content: String.duplicate("x", 100) <> "#{i}",
        type: "application/octet-stream"
      }
    end
  end

  describe "BucketLive uploads" do
    test "a batch under the cap uploads every file" do
      {:ok, lv, _html} = live(build_conn(), "/buckets/upbucket")

      # One file_input per drop: the test UploadClient's shared socket closes
      # once the server consumes an entry (consume-on-done in handle_progress),
      # so multi-entry structs can't be driven to completion sequentially.
      # Entries are consumed on completion, so assert the success flash
      # rather than a lingering 100% bar.
      for i <- 1..3 do
        input = file_input(lv, "#upload-form", :files, [Enum.at(entries(3), i - 1)])
        assert render_upload(input, "file-#{i}.bin") =~ "uploaded file-#{i}.bin"
      end

      {keys, _cps, _trunc, _next} = Index.list("upbucket", max_keys: 10)
      names = Enum.map(keys, & &1.key) |> Enum.sort()
      assert names == ["file-1.bin", "file-2.bin", "file-3.bin"]
    end

    test "files beyond the batch cap are cancelled with a visible notice, not stuck" do
      Application.put_env(:kafun, :admin_max_upload_files, 3)

      {:ok, lv, _html} = live(build_conn(), "/buckets/upbucket")

      input = file_input(lv, "#upload-form", :files, entries(5))

      # On the wire, the change event carries the upload metadata and the
      # channel registers entries (put_entries → :too_many_files) *before*
      # dispatching validate. LiveViewTest splits those into two calls.
      assert {:ok, _} = preflight_upload(input)
      html = render_change(lv, "validate", %{})

      assert html =~ "2 file(s) skipped"
      assert html =~ "max 3 per batch"

      # The surplus are gone from the pending list (not stuck at 0%), and
      # the config-level error cleared with them.
      html = render(lv)
      refute html =~ "file-4.bin"
      refute html =~ "file-5.bin"
      refute html =~ "too many files in this batch"

      # Clear the three still-pending survivors via their ✕ buttons (also
      # exercises the cancel path), then prove a fresh drop goes through.
      for ref <- Regex.scan(~r/phx-value-ref="([^"]+)"/, html) |> Enum.map(&Enum.at(&1, 1)) do
        render_click(lv, "cancel-upload", %{"ref" => ref})
      end

      assert render(lv) =~ "No objects under this prefix."

      fresh = file_input(lv, "#upload-form", :files, [%{name: "after.bin", content: "ok"}])
      assert render_upload(fresh, "after.bin") =~ "uploaded after.bin"

      {keys, _cps, _trunc, _next} = Index.list("upbucket", max_keys: 10)
      assert Enum.map(keys, & &1.key) == ["after.bin"]
    end
  end
end
