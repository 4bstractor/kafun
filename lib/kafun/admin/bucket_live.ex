defmodule Kafun.Admin.BucketLive do
  @moduledoc "Per-bucket browser. Paginated, prefix-aware via the listing scanner."

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico)

  alias Kafun.Index

  @page_size 100

  @impl true
  def mount(%{"bucket" => bucket}, _session, socket) do
    if Index.bucket_exists?(bucket) do
      {:ok,
       assign(socket,
         bucket: bucket,
         prefix: "",
         notice: nil,
         page_size_label: @page_size
       )
       |> load_page("")}
    else
      {:ok, push_navigate(socket, to: ~p"/buckets")}
    end
  end

  @impl true
  def handle_params(params, _uri, socket) do
    prefix = Map.get(params, "prefix", "")
    {:noreply, socket |> assign(prefix: prefix) |> load_page(prefix)}
  end

  @impl true
  def handle_event("delete", %{"key" => key}, socket) do
    :ok = Kafun.Storage.delete(root(), socket.assigns.bucket, key)
    :ok = Index.delete(socket.assigns.bucket, key)

    {:noreply,
     socket
     |> assign(notice: {:info, "deleted #{key}"})
     |> load_page(socket.assigns.prefix)}
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
