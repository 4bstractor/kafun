defmodule Kafun.Admin.UploadsLive do
  @moduledoc "In-flight multipart uploads with abort buttons."

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico)

  alias Kafun.{Index, Multipart}

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, rows: Index.list_all_uploads(), notice: nil)}
  end

  @impl true
  def handle_event("abort", %{"upload-id" => upload_id}, socket) do
    case Multipart.abort(root(), upload_id) do
      :ok ->
        {:noreply,
         assign(socket,
           rows: Index.list_all_uploads(),
           notice: {:info, "aborted #{String.slice(upload_id, 0..7)}…"}
         )}

      {:error, :no_such_upload} ->
        {:noreply,
         assign(socket,
           rows: Index.list_all_uploads(),
           notice: {:error, "upload no longer exists (already finished or GC'd)"}
         )}
    end
  end

  defp root, do: Application.fetch_env!(:kafun, :root)

  defp format_date(unix), do: unix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M:%S")

  defp age_seconds(unix), do: System.system_time(:second) - unix

  defp humanize_age(s) when s < 60, do: "#{s}s"
  defp humanize_age(s) when s < 3600, do: "#{div(s, 60)}m"
  defp humanize_age(s) when s < 86_400, do: "#{div(s, 3600)}h #{div(rem(s, 3600), 60)}m"
  defp humanize_age(s), do: "#{div(s, 86_400)}d #{div(rem(s, 86_400), 3600)}h"

  @impl true
  def render(assigns) do
    ~H"""
    <h1>Multipart uploads in flight</h1>

    <%= if @notice do %>
      <div class={"flash flash-#{elem(@notice, 0)}"}>{elem(@notice, 1)}</div>
    <% end %>

    <p style="color: #8b949e;">
      Uploads listed here are mid-flight or stuck (e.g., a client that PUT some parts
      then never called CompleteMultipartUpload). Aborting deletes the staged part
      bytes from disk and clears the index row. The GC also cleans these up
      automatically once they're older than <code>KAFUN_GC_ABANDON_AFTER_SEC</code>.
    </p>

    <%= if @rows == [] do %>
      <div class="empty">No multipart uploads currently in flight.</div>
    <% else %>
      <table>
        <thead>
          <tr>
            <th>Bucket / Key</th>
            <th>Upload ID</th>
            <th>Started</th>
            <th class="num">Age</th>
            <th class="num">Parts</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <%= for u <- @rows do %>
            <tr>
              <td>
                <.link navigate={~p"/buckets/#{u.bucket}"}>{u.bucket}</.link>
                / <span style="color: #c9d1d9;">{u.key}</span>
              </td>
              <td><code>{String.slice(u.upload_id, 0..15)}…</code></td>
              <td>{format_date(u.initiated_at)}</td>
              <td class="num">{humanize_age(age_seconds(u.initiated_at))}</td>
              <td class="num">{u.parts}</td>
              <td>
                <button class="btn btn-danger" phx-click="abort"
                        phx-value-upload-id={u.upload_id}
                        data-confirm={"Abort upload #{String.slice(u.upload_id, 0..7)}…? Staged parts will be deleted."}>
                  Abort
                </button>
              </td>
            </tr>
          <% end %>
        </tbody>
      </table>
    <% end %>
    """
  end
end
