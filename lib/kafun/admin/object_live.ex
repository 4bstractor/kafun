defmodule Kafun.Admin.ObjectLive do
  @moduledoc "Per-object detail page. Inline preview for images, metadata view + edit, rename, delete."

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico favicon.png)

  alias Kafun.{Index, Storage}

  @impl true
  def mount(%{"bucket" => bucket, "key_parts" => parts}, _session, socket) do
    key = Enum.join(parts, "/")

    case Index.get(bucket, key) do
      {:ok, meta} ->
        {:ok,
         assign(socket,
           bucket: bucket,
           key: key,
           meta: meta,
           edit_mode: nil,
           form_content_type: meta.content_type || "",
           form_meta_text: meta_to_text(Map.get(meta, :meta, %{})),
           form_new_key: key,
           notice: nil
         )}

      :not_found ->
        {:ok, push_navigate(socket, to: ~p"/buckets/#{bucket}")}
    end
  end

  @impl true
  def handle_event("delete", _params, socket) do
    :ok = Storage.delete(root(), socket.assigns.bucket, socket.assigns.key)
    :ok = Index.delete(socket.assigns.bucket, socket.assigns.key)

    parent = parent_prefix(socket.assigns.key)

    {:noreply,
     push_navigate(socket,
       to: ~p"/buckets/#{socket.assigns.bucket}?prefix=#{parent}"
     )}
  end

  def handle_event("edit", %{"mode" => mode}, socket) do
    {:noreply, assign(socket, edit_mode: mode, notice: nil)}
  end

  def handle_event("cancel", _params, socket) do
    meta = socket.assigns.meta

    {:noreply,
     assign(socket,
       edit_mode: nil,
       form_content_type: meta.content_type || "",
       form_meta_text: meta_to_text(Map.get(meta, :meta, %{})),
       form_new_key: socket.assigns.key,
       notice: nil
     )}
  end

  def handle_event("save_meta", %{"content_type" => ct, "meta_text" => txt}, socket) do
    case parse_meta_text(txt) do
      {:ok, new_meta} ->
        cur = socket.assigns.meta
        new_ct = if ct == "", do: nil, else: ct

        :ok =
          Index.put(
            socket.assigns.bucket,
            socket.assigns.key,
            cur.size,
            cur.etag,
            new_ct,
            System.system_time(:second),
            new_meta
          )

        {:ok, refreshed} = Index.get(socket.assigns.bucket, socket.assigns.key)

        {:noreply,
         assign(socket,
           meta: refreshed,
           edit_mode: nil,
           form_content_type: new_ct || "",
           form_meta_text: meta_to_text(new_meta),
           notice: {:info, "metadata updated"}
         )}

      {:error, line_num, line} ->
        {:noreply,
         assign(socket,
           notice: {:error, "line #{line_num}: malformed (#{line}) — expected `name: value`"}
         )}
    end
  end

  def handle_event("save_rename", %{"new_key" => new_key}, socket) do
    new_key = String.trim(new_key)
    bucket = socket.assigns.bucket
    old_key = socket.assigns.key

    cond do
      new_key == old_key ->
        {:noreply, assign(socket, edit_mode: nil)}

      not Storage.valid_key?(new_key) ->
        {:noreply, assign(socket, notice: {:error, "invalid destination key"})}

      match?({:ok, _}, Index.get(bucket, new_key)) ->
        {:noreply, assign(socket, notice: {:error, "destination key already exists"})}

      true ->
        cur = socket.assigns.meta

        case Storage.copy_blob(root(), bucket, old_key, bucket, new_key) do
          {:ok, _} ->
            :ok =
              Index.put(
                bucket,
                new_key,
                cur.size,
                cur.etag,
                cur.content_type,
                System.system_time(:second),
                Map.get(cur, :meta, %{})
              )

            :ok = Storage.delete(root(), bucket, old_key)
            :ok = Index.delete(bucket, old_key)

            {:noreply, push_navigate(socket, to: ~p"/buckets/#{bucket}/#{new_key}")}

          {:error, :not_found} ->
            {:noreply, assign(socket, notice: {:error, "source blob is missing"})}
        end
    end
  end

  ## Helpers

  defp root, do: Application.fetch_env!(:kafun, :root)

  defp parent_prefix(key) do
    case Path.dirname(key) do
      "." -> ""
      "/" -> ""
      d -> d <> "/"
    end
  end

  defp humanize_bytes(n) when n < 1024, do: "#{n} B"
  defp humanize_bytes(n) when n < 1024 * 1024, do: "#{Float.round(n / 1024, 1)} KiB"
  defp humanize_bytes(n) when n < 1024 * 1024 * 1024, do: "#{Float.round(n / (1024 * 1024), 1)} MiB"
  defp humanize_bytes(n), do: "#{Float.round(n / (1024 * 1024 * 1024), 2)} GiB"

  defp format_date(unix), do: unix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M:%S")

  defp image?(%{content_type: "image/" <> _}, _), do: true

  defp image?(%{content_type: ct}, key) when ct in [nil, ""] do
    String.starts_with?(MIME.from_path(key), "image/")
  end

  defp image?(_, _), do: false

  # Image previews work by emitting an `<img src=…>` that the *browser*
  # has to fetch. The browser is some other machine, so we can't use
  # `localhost`. `KAFUN_PUBLIC_S3_URL` (set in env) is the externally
  # reachable URL of the S3 surface — typically the public NPM hostname.
  # When unset we fall back to `KAFUN_HOST:KAFUN_PORT`, which works for
  # local dev where the browser and the service share a host.
  defp preview_url(bucket, key) do
    base_url = s3_public_base()
    base = "#{base_url}/#{bucket}/#{URI.encode(key)}"

    case Application.fetch_env!(:kafun, :allowed_keys) do
      %MapSet{} = keys ->
        case MapSet.to_list(keys) do
          [] ->
            base

          [access_key | _] ->
            base <> "?X-Amz-Credential=#{access_key}/admin/us-east-1/s3/aws4_request"
        end
    end
  end

  defp s3_public_base do
    case Application.get_env(:kafun, :public_s3_url) do
      url when is_binary(url) and url != "" ->
        String.trim_trailing(url, "/")

      _ ->
        host = Application.get_env(:kafun, :host, "0.0.0.0")
        host = if host == "0.0.0.0", do: "localhost", else: host
        port = Application.get_env(:kafun, :port, 8333)
        "http://#{host}:#{port}"
    end
  end

  defp meta_to_text(%{} = m) when map_size(m) == 0, do: ""

  defp meta_to_text(%{} = m) do
    m |> Enum.sort() |> Enum.map_join("\n", fn {k, v} -> "#{k}: #{v}" end)
  end

  # Parse `key: value` per line. Empty lines ignored. Returns
  # `{:ok, map}` or `{:error, line_num, raw_line}`.
  defp parse_meta_text(txt) do
    txt
    |> String.split(["\n", "\r\n"])
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, %{}}, fn {raw, idx}, {:ok, acc} ->
      line = String.trim(raw)

      cond do
        line == "" ->
          {:cont, {:ok, acc}}

        not String.contains?(line, ":") ->
          {:halt, {:error, idx, raw}}

        true ->
          [k, v] = String.split(line, ":", parts: 2)
          k = String.trim(k)
          v = String.trim(v)

          if k == "" do
            {:halt, {:error, idx, raw}}
          else
            {:cont, {:ok, Map.put(acc, k, v)}}
          end
      end
    end)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div class="breadcrumbs">
      <.link navigate={~p"/buckets"}>buckets</.link>
      <.link navigate={~p"/buckets/#{@bucket}"}>{@bucket}</.link>
      <span>{@key}</span>
    </div>

    <h1>{Path.basename(@key)}</h1>

    <%= if @notice do %>
      <div class={"flash flash-#{elem(@notice, 0)}"}>{elem(@notice, 1)}</div>
    <% end %>

    <%= if image?(@meta, @key) do %>
      <div class="preview" style="margin-bottom: 1.25rem;">
        <img src={preview_url(@bucket, @key)} alt={@key} />
      </div>
    <% end %>

    <h2>Metadata</h2>
    <dl class="meta-grid">
      <dt>Size</dt><dd>{humanize_bytes(@meta.size)} <span class="pill">{@meta.size} bytes</span></dd>
      <dt>ETag</dt><dd>{@meta.etag}</dd>
      <dt>Content-Type</dt><dd>{@meta.content_type || "—"}</dd>
      <dt>Last modified</dt><dd>{format_date(@meta.mtime)}</dd>
      <dt>Full key</dt><dd>{@key}</dd>
    </dl>

    <%= if Map.get(@meta, :meta, %{}) != %{} do %>
      <h2>User metadata (x-amz-meta-*)</h2>
      <dl class="meta-grid">
        <%= for {k, v} <- Enum.sort(@meta.meta) do %>
          <dt>{k}</dt>
          <dd>{v}</dd>
        <% end %>
      </dl>
    <% end %>

    <h2>Actions</h2>
    <div class="row">
      <a class="btn" href={preview_url(@bucket, @key)} target="_blank" rel="noopener">Open / download</a>
      <button class="btn" phx-click="edit" phx-value-mode="meta">Edit metadata</button>
      <button class="btn" phx-click="edit" phx-value-mode="rename">Rename / move</button>
      <button class="btn btn-danger" phx-click="delete"
              data-confirm={"Delete #{@key}? This cannot be undone."}>
        Delete object
      </button>
    </div>

    <%= case @edit_mode do %>
      <% "meta" -> %>
        <h2>Edit metadata</h2>
        <form phx-submit="save_meta" class="stack" style="max-width: 640px;">
          <div>
            <label>Content-Type</label>
            <input type="text" name="content_type" value={@form_content_type}
                   placeholder="image/png · text/plain · application/json" />
          </div>
          <div>
            <label>User metadata (one <code>name: value</code> per line, names land as <code>x-amz-meta-&lt;name&gt;</code>)</label>
            <textarea name="meta_text" rows="6" style="font-family: ui-monospace, monospace;">{@form_meta_text}</textarea>
          </div>
          <div class="row">
            <button class="btn btn-primary" type="submit">Save</button>
            <button class="btn" type="button" phx-click="cancel">Cancel</button>
          </div>
        </form>

      <% "rename" -> %>
        <h2>Rename / move</h2>
        <form phx-submit="save_rename" class="stack" style="max-width: 640px;">
          <div>
            <label>New key (within the same bucket)</label>
            <input type="text" name="new_key" value={@form_new_key} />
          </div>
          <div class="row">
            <button class="btn btn-primary" type="submit">Rename</button>
            <button class="btn" type="button" phx-click="cancel">Cancel</button>
          </div>
        </form>

      <% _ -> %>
    <% end %>
    """
  end
end
