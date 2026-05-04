defmodule Kafun.Admin.BucketLive do
  @moduledoc "Per-bucket browser. Paginated, prefix-aware via the listing scanner. Drag-and-drop upload."

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico favicon.png)

  alias Kafun.{Index, Storage}

  @page_size 100

  @impl true
  def mount(%{"bucket" => bucket}, _session, socket) do
    if Index.bucket_exists?(bucket) do
      max_mb = Application.get_env(:kafun, :admin_max_upload_mb, 256)

      socket =
        socket
        |> assign(
          bucket: bucket,
          prefix: "",
          upload_prefix: "",
          notice: nil,
          page_size_label: @page_size,
          max_upload_mb: max_mb
        )
        |> allow_upload(:files,
          accept: :any,
          max_entries: 50,
          max_file_size: max_mb * 1024 * 1024,
          auto_upload: true,
          progress: &handle_progress/3
        )
        |> load_page("")

      {:ok, socket}
    else
      {:ok, push_navigate(socket, to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(params, _uri, socket) do
    prefix = Map.get(params, "prefix", "")

    socket =
      socket
      |> assign(prefix: prefix, upload_prefix: prefix)
      |> load_page(prefix)

    {:noreply, socket}
  end

  @impl true
  def handle_event("delete", %{"key" => key}, socket) do
    :ok = Storage.delete(root(), socket.assigns.bucket, key)
    :ok = Index.delete(socket.assigns.bucket, key)

    {:noreply,
     socket
     |> assign(notice: {:info, "deleted #{key}"})
     |> load_page(socket.assigns.prefix)}
  end

  def handle_event("set-upload-prefix", %{"upload_prefix" => p}, socket) do
    {:noreply, assign(socket, upload_prefix: p)}
  end

  def handle_event("validate", _params, socket), do: {:noreply, socket}

  def handle_event("cancel-upload", %{"ref" => ref}, socket) do
    {:noreply, cancel_upload(socket, :files, ref)}
  end

  ## Phoenix LiveView upload progress callback. Fires on every progress
  ## tick; `entry.done?` is the signal to consume + import.
  defp handle_progress(:files, entry, socket) do
    if entry.done? do
      result =
        consume_uploaded_entry(socket, entry, fn %{path: tmp} ->
          {:ok, import_one(socket, entry, tmp)}
        end)

      {notice, reload?} =
        case result do
          {:ok, key, _size} ->
            {{:info, "uploaded #{key}"}, true}

          {:conflict, key} ->
            {{:error, "#{key} already exists — delete or rename first"}, false}

          {:invalid, key} ->
            {{:error, "invalid key #{inspect(key)} — keys can't start with /, contain control bytes, or use ./.. segments"}, false}
        end

      socket = assign(socket, notice: notice)
      socket = if reload?, do: load_page(socket, socket.assigns.prefix), else: socket
      {:noreply, socket}
    else
      {:noreply, socket}
    end
  end

  defp import_one(socket, entry, tmp_path) do
    bucket = socket.assigns.bucket
    upload_prefix = normalize_prefix(socket.assigns.upload_prefix)
    filename = entry.client_name
    key = Path.join(upload_prefix, filename) |> String.trim_leading("/")

    cond do
      not Storage.valid_key?(key) ->
        {:invalid, key}

      match?({:ok, _}, Index.get(bucket, key)) ->
        {:conflict, key}

      true ->
        {:ok, size, etag} = Storage.import_file(root(), bucket, key, tmp_path)
        ct = entry.client_type || MIME.from_path(filename)
        :ok = Index.put(bucket, key, size, etag, ct, System.system_time(:second), %{})
        {:ok, key, size}
    end
  end

  defp normalize_prefix(""), do: ""
  defp normalize_prefix(nil), do: ""

  defp normalize_prefix(p) do
    p
    |> String.trim()
    |> String.trim_leading("/")
    |> then(fn s -> if s == "" or String.ends_with?(s, "/"), do: s, else: s <> "/" end)
  end

  defp load_page(socket, prefix) do
    {entries, common_prefixes, truncated?, _next} =
      Index.list(socket.assigns.bucket,
        prefix: prefix,
        delimiter: "/",
        max_keys: @page_size
      )

    assign(socket,
      entries: entries,
      common_prefixes: common_prefixes,
      truncated: truncated?
    )
  end

  defp root, do: Application.fetch_env!(:kafun, :root)

  defp humanize_bytes(n) when n < 1024, do: "#{n} B"
  defp humanize_bytes(n) when n < 1024 * 1024, do: "#{Float.round(n / 1024, 1)} KiB"
  defp humanize_bytes(n) when n < 1024 * 1024 * 1024, do: "#{Float.round(n / (1024 * 1024), 1)} MiB"
  defp humanize_bytes(n), do: "#{Float.round(n / (1024 * 1024 * 1024), 2)} GiB"

  defp format_date(unix), do: unix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M")

  defp parent_prefix(""), do: nil

  defp parent_prefix(prefix) do
    case prefix |> String.trim_trailing("/") |> Path.dirname() do
      "." -> ""
      "/" -> ""
      p -> p <> "/"
    end
  end

  defp upload_error_text(:too_large), do: "exceeds size cap"
  defp upload_error_text(:not_accepted), do: "rejected file type"
  defp upload_error_text(:too_many_files), do: "too many files in this batch"
  defp upload_error_text(other), do: to_string(other)

  @impl true
  def render(assigns) do
    ~H"""
    <div class="breadcrumbs">
      <.link navigate={~p"/buckets"}>buckets</.link>
      <.link patch={~p"/buckets/#{@bucket}"}>{@bucket}</.link>
      <%= for seg <- prefix_segments(@prefix) do %>
        <.link patch={~p"/buckets/#{@bucket}?prefix=#{seg.path}"}>{seg.label}</.link>
      <% end %>
    </div>

    <%= if @notice do %>
      <div class={"flash flash-#{elem(@notice, 0)}"}>{elem(@notice, 1)}</div>
    <% end %>

    <h1>{@bucket}{if @prefix != "", do: " · " <> @prefix}</h1>

    <%= if parent = parent_prefix(@prefix) do %>
      <p>
        <.link patch={~p"/buckets/#{@bucket}?prefix=#{parent}"}>↰ up one level</.link>
      </p>
    <% end %>

    <form id="upload-form" phx-change="validate" phx-submit="validate" class="upload-block">
      <div class="upload-prefix-row">
        <label for="upload_prefix">Upload to prefix:</label>
        <input type="text" name="upload_prefix" value={@upload_prefix}
               placeholder="(bucket root — type a path like 'void-mate/profile-3/' to nest)"
               phx-change="set-upload-prefix"
               phx-debounce="200" />
      </div>

      <label class="dropzone" phx-drop-target={@uploads.files.ref}>
        <div class="dropzone-headline">Drop files here, or click to choose.</div>
        <div class="dropzone-sub">
          Max {@max_upload_mb} MiB per file. Filename becomes the key, prepended with the prefix above.
          Conflicts on existing keys are refused — delete the old object first to replace it.
        </div>
        <.live_file_input upload={@uploads.files} />
      </label>

      <%= if @uploads.files.entries != [] do %>
        <div class="upload-list">
          <%= for entry <- @uploads.files.entries do %>
            <div class="upload-row">
              <span class="upload-name">{entry.client_name}</span>
              <progress max="100" value={entry.progress} class="upload-bar" />
              <span class="upload-pct">{entry.progress}%</span>
              <button type="button"
                      class="btn upload-cancel"
                      phx-click="cancel-upload"
                      phx-value-ref={entry.ref}>
                ✕
              </button>
              <%= for err <- upload_errors(@uploads.files, entry) do %>
                <span class="upload-err">{upload_error_text(err)}</span>
              <% end %>
            </div>
          <% end %>
        </div>
      <% end %>
    </form>

    <%= if @entries == [] and @common_prefixes == [] do %>
      <div class="empty">No objects under this prefix.</div>
    <% else %>
      <table>
        <thead>
          <tr>
            <th>Key</th>
            <th>Modified</th>
            <th class="num">Size</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <%= for cp <- @common_prefixes do %>
            <tr>
              <td>
                📁 <.link patch={~p"/buckets/#{@bucket}?prefix=#{cp}"}>{relative_label(cp, @prefix)}</.link>
              </td>
              <td>—</td>
              <td class="num">—</td>
              <td></td>
            </tr>
          <% end %>
          <%= for e <- @entries do %>
            <tr>
              <td>
                <.link navigate={~p"/buckets/#{@bucket}/#{e.key}"}>{relative_label(e.key, @prefix)}</.link>
              </td>
              <td>{format_date(e.mtime)}</td>
              <td class="num">{humanize_bytes(e.size)}</td>
              <td>
                <button class="btn btn-danger" phx-click="delete"
                        phx-value-key={e.key}
                        data-confirm={"Delete #{e.key}?"}>
                  Delete
                </button>
              </td>
            </tr>
          <% end %>
        </tbody>
      </table>
      <%= if @truncated do %>
        <p style="color: #8b949e; margin-top: 0.5rem;">
          Showing first {@page_size_label} entries. (Pagination wiring is a follow-up.)
        </p>
      <% end %>
    <% end %>
    """
  end

  defp relative_label(full, ""), do: full

  defp relative_label(full, prefix) do
    if String.starts_with?(full, prefix) do
      String.slice(full, byte_size(prefix), byte_size(full) - byte_size(prefix))
    else
      full
    end
  end

  defp prefix_segments(""), do: []

  defp prefix_segments(prefix) do
    parts = String.split(String.trim_trailing(prefix, "/"), "/")

    parts
    |> Enum.with_index()
    |> Enum.map(fn {label, i} ->
      path = Enum.take(parts, i + 1) |> Enum.join("/") |> Kernel.<>("/")
      %{label: label, path: path}
    end)
  end
end
