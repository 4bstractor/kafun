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
      max_files = Application.get_env(:kafun, :admin_max_upload_files, 50)

      socket =
        socket
        |> assign(
          bucket: bucket,
          prefix: "",
          upload_prefix: "",
          notice: nil,
          page_size_label: @page_size,
          max_upload_mb: max_mb,
          max_upload_files: max_files,
          upload_report: nil
        )
        |> allow_upload(:files,
          accept: :any,
          max_entries: max_files,
          max_file_size: max_mb * 1024 * 1024,
          auto_upload: true,
          progress: &handle_progress/3
        )
        |> load_page("")
        |> load_perms()

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

  # The BatchedUpload hook announces the full selection size before feeding
  # the first wave; from here on skips/conflicts accumulate in the report
  # panel instead of flash churn.
  def handle_event("upload-batch-start", %{"total" => total}, socket) when is_integer(total) do
    {:noreply, assign(socket, upload_report: %{total: total, done: 0, failed: []}, notice: nil)}
  end

  def handle_event("validate", _params, socket) do
    conf = socket.assigns.uploads.files

    # Two kinds of entry never make progress and would sit at 0% forever if
    # left alone: entries with per-entry errors (too_large, not_accepted) in
    # auto_upload mode, and the surplus beyond max_entries — those carry
    # :too_many_files as a *config-level* error keyed to the upload ref, not
    # to any entry, so they get no token and no visible error row. Cancel
    # both kinds and record why.
    errored =
      for entry <- conf.entries, upload_errors(conf, entry) != [] do
        {entry, conf |> upload_errors(entry) |> Enum.map_join(", ", &upload_error_text/1)}
      end

    surplus =
      if :too_many_files in upload_errors(conf) do
        skipped = Enum.drop(conf.entries, conf.max_entries) -- Enum.map(errored, &elem(&1, 0))
        for entry <- skipped, do: {entry, "over the per-wave cap"}
      else
        []
      end

    case errored ++ surplus do
      [] ->
        {:noreply, socket}

      cancels ->
        socket =
          Enum.reduce(cancels, socket, fn {entry, _}, s -> cancel_upload(s, :files, entry.ref) end)

        socket =
          case socket.assigns.upload_report do
            nil ->
              assign(socket,
                notice:
                  {:error,
                   "#{length(cancels)} file(s) skipped — max #{conf.max_entries} per batch. " <>
                     "Re-drop the skipped files once this batch finishes."}
              )

            report ->
              failed = report.failed ++ Enum.map(cancels, fn {e, why} -> {e.client_name, why} end)
              assign(socket, upload_report: %{report | failed: failed})
          end

        {:noreply, maybe_wave_done(socket)}
    end
  end

  def handle_event("cancel-upload", %{"ref" => ref}, socket) do
    {:noreply, cancel_upload(socket, :files, ref)}
  end

  ## Permissions panel

  def handle_event("toggle_public_read", _params, socket) do
    new_state = not socket.assigns.public_read
    :ok = Index.set_bucket_public_read(socket.assigns.bucket, new_state)

    notice =
      if new_state,
        do: {:info, "anonymous read access enabled"},
        else: {:info, "anonymous read access disabled"}

    {:noreply, socket |> load_perms() |> assign(notice: notice)}
  end

  def handle_event("add_grant", %{"access_key_id" => key_id, "permission" => perm}, socket) do
    cond do
      key_id == "" ->
        {:noreply, assign(socket, notice: {:error, "pick an access key first"})}

      perm not in ~w(read write admin) ->
        {:noreply, assign(socket, notice: {:error, "invalid permission"})}

      true ->
        :ok = Index.upsert_grant(key_id, socket.assigns.bucket, String.to_atom(perm))

        {:noreply,
         socket
         |> load_perms()
         |> assign(notice: {:info, "granted #{perm} on #{socket.assigns.bucket} to #{mask_id(key_id)}"})}
    end
  end

  def handle_event("delete_grant", %{"access_key_id" => key_id}, socket) do
    :ok = Index.delete_grant(key_id, socket.assigns.bucket)

    {:noreply,
     socket
     |> load_perms()
     |> assign(notice: {:info, "revoked grant for #{mask_id(key_id)}"})}
  end

  ## Phoenix LiveView upload progress callback. Fires on every progress
  ## tick; `entry.done?` is the signal to consume + import.
  defp handle_progress(:files, entry, socket) do
    if entry.done? do
      result =
        consume_uploaded_entry(socket, entry, fn %{path: tmp} ->
          {:ok, import_one(socket, entry, tmp)}
        end)

      socket =
        case socket.assigns.upload_report do
          # No batch announced (JS hook absent) — legacy per-file flash.
          nil -> flash_result(socket, result)
          report -> assign(socket, upload_report: record_result(report, result))
        end

      # consume_uploaded_entry drops the entry from the channel state only
      # after this callback returns, so exclude it from the drain check here.
      {:noreply, maybe_wave_done(socket, entry.ref)}
    else
      {:noreply, socket}
    end
  end

  defp flash_result(socket, result) do
    notice =
      case result do
        {:ok, key, _size} ->
          {:info, "uploaded #{key}"}

        {:conflict, key} ->
          {:error, "#{key} already exists — delete or rename first"}

        {:invalid, key} ->
          {:error, "invalid key #{inspect(key)} — keys can't start with /, contain control bytes, or use ./.. segments"}
      end

    assign(socket, notice: notice)
  end

  defp record_result(report, result) do
    case result do
      {:ok, _key, _size} -> %{report | done: report.done + 1}
      {:conflict, key} -> %{report | failed: report.failed ++ [{key, "already exists"}]}
      {:invalid, key} -> %{report | failed: report.failed ++ [{key, "invalid key"}]}
    end
  end

  # A wave is drained when no entries are pending. Refresh the listing,
  # finish the report if the whole batch is accounted for, and tell the
  # BatchedUpload hook to feed the next wave.
  defp maybe_wave_done(socket, consumed_ref \\ nil) do
    if Enum.all?(socket.assigns.uploads.files.entries, &(&1.ref == consumed_ref)) do
      socket = load_page(socket, socket.assigns.prefix)

      socket =
        case socket.assigns.upload_report do
          %{total: total, done: done, failed: failed} when done + length(failed) >= total ->
            notice =
              if failed == [] do
                {:info, "uploaded #{done} file(s)"}
              else
                {:error, "uploaded #{done} file(s), skipped #{length(failed)} — details below"}
              end

            assign(socket, notice: notice)

          _ ->
            socket
        end

      push_event(socket, "upload-wave-done", %{})
    else
      socket
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

  defp load_perms(socket) do
    bucket = socket.assigns.bucket

    assign(socket,
      public_read: Index.bucket_public_read?(bucket),
      grants: Index.list_bucket_grants(bucket),
      available_keys:
        Index.list_access_keys() |> Enum.filter(&(&1.status == :active))
    )
  end

  defp mask_id(id) when byte_size(id) >= 8 do
    String.slice(id, 0..3) <> "…" <> String.slice(id, -4..-1//1)
  end

  defp mask_id(id), do: id

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

      <label id="upload-dropzone" class="dropzone" phx-hook="BatchedUpload"
             data-wave-size={@max_upload_files}>
        <div class="dropzone-headline">Drop files here, or click to choose.</div>
        <div class="dropzone-sub">
          Any number of files, uploaded {@max_upload_files} at a time; max {@max_upload_mb} MiB per file.
          Filename becomes the key, prepended with the prefix above.
          Conflicts on existing keys are skipped and listed — delete the old object first to replace it.
        </div>
        <input type="file" id="upload-picker" multiple phx-update="ignore" />
        <.live_file_input upload={@uploads.files} style="display: none;" />
      </label>

      <%= if @upload_report do %>
        <div class="upload-report">
          <%= if @upload_report.done + length(@upload_report.failed) < @upload_report.total do %>
            <div class="upload-report-progress">
              Uploading… {@upload_report.done + length(@upload_report.failed)} / {@upload_report.total}
              <%= if @upload_report.failed != [] do %>
                ({length(@upload_report.failed)} skipped so far)
              <% end %>
            </div>
          <% end %>
          <%= if @upload_report.failed != [] do %>
            <details class="upload-failed" open={@upload_report.done + length(@upload_report.failed) >= @upload_report.total}>
              <summary>{length(@upload_report.failed)} skipped file(s)</summary>
              <ul>
                <%= for {name, reason} <- @upload_report.failed do %>
                  <li><code>{name}</code> — {reason}</li>
                <% end %>
              </ul>
            </details>
          <% end %>
        </div>
      <% end %>

      <%= for err <- upload_errors(@uploads.files) do %>
        <div class="upload-err">{upload_error_text(err)}</div>
      <% end %>

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

    <h2>Permissions</h2>

    <div class="perms-block">
      <div class="perms-public">
        <label class="perms-public-label">
          <input type="checkbox"
                 phx-click="toggle_public_read"
                 checked={@public_read} />
          <strong>Public read access</strong> —
          allows anonymous (unauthenticated) GET / HEAD / list on this bucket.
          Writes always require an authenticated key with a grant.
        </label>
      </div>

      <h3>Grants</h3>
      <%= if @grants == [] do %>
        <div class="empty" style="padding: 1rem;">No per-key grants on this bucket.</div>
      <% else %>
        <table>
          <thead>
            <tr>
              <th>Access key</th>
              <th>Permission</th>
              <th>Granted</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            <%= for g <- @grants do %>
              <tr>
                <td><code>{mask_id(g.access_key_id)}</code> <span class="key-full">({g.access_key_id})</span></td>
                <td><span class="pill">{g.permission}</span></td>
                <td>{format_date(g.granted_at)}</td>
                <td>
                  <button class="btn btn-danger" phx-click="delete_grant"
                          phx-value-access_key_id={g.access_key_id}
                          data-confirm={"Remove the #{g.permission} grant from #{mask_id(g.access_key_id)} on #{@bucket}?"}>
                    Remove
                  </button>
                </td>
              </tr>
            <% end %>
          </tbody>
        </table>
      <% end %>

      <h3>Add grant</h3>
      <%= if @available_keys == [] do %>
        <p style="color: #8b949e;">
          No active access keys to grant. Generate one on the
          <.link navigate={~p"/keys"}>Keys page</.link> first.
        </p>
      <% else %>
        <form phx-submit="add_grant" class="row" style="margin-bottom: 1rem; gap: 0.5rem;">
          <select name="access_key_id" style="max-width: 360px;">
            <option value="">— select an access key —</option>
            <%= for k <- @available_keys do %>
              <option value={k.id}>
                {mask_id(k.id)}{if k.description != "", do: " · " <> k.description, else: ""}
              </option>
            <% end %>
          </select>
          <select name="permission">
            <option value="read">read</option>
            <option value="write">write</option>
            <option value="admin">admin</option>
          </select>
          <button type="submit" class="btn btn-primary">Grant</button>
        </form>
        <p style="color: #8b949e; font-size: 0.85rem;">
          Tip: a global grant on bucket <code>*</code> applies to every bucket and
          is best edited via <code>kafun rpc</code> for now (Index.upsert_grant/3).
        </p>
      <% end %>
    </div>
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
