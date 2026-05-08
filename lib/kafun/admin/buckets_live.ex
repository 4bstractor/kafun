defmodule Kafun.Admin.BucketsLive do
  @moduledoc "Buckets index. List with stats, create new, delete empty."

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico favicon.png)

  alias Kafun.{Index, Storage}

  @impl true
  def mount(_params, _session, socket) do
    {:ok, assign(socket, buckets: Index.bucket_stats(), notice: nil, new_name: "")}
  end

  @impl true
  def handle_event("create", %{"name" => name}, socket) do
    name = String.trim(name)

    cond do
      name == "" ->
        {:noreply, assign(socket, notice: {:error, "bucket name cannot be empty"})}

      not Storage.valid_bucket?(name) ->
        {:noreply,
         assign(socket,
           notice: {:error, "invalid bucket name (must match /^[a-z0-9][a-z0-9.-]{1,62}$/)"}
         )}

      Index.bucket_exists?(name) ->
        {:noreply, assign(socket, notice: {:info, "bucket #{name} already exists"})}

      true ->
        :ok = Index.ensure_bucket(name)
        File.mkdir_p!(Path.join(root(), name))

        {:noreply,
         assign(socket,
           buckets: Index.bucket_stats(),
           new_name: "",
           notice: {:info, "created bucket #{name}"}
         )}
    end
  end

  def handle_event("delete", %{"name" => name}, socket) do
    case Index.delete_bucket(name) do
      :ok ->
        _ = File.rmdir(Path.join(root(), name))

        {:noreply,
         assign(socket, buckets: Index.bucket_stats(), notice: {:info, "deleted #{name}"})}

      {:error, :not_empty} ->
        {:noreply,
         assign(socket, notice: {:error, "#{name} is not empty — delete its objects first"})}

      {:error, :not_found} ->
        {:noreply, assign(socket, notice: {:error, "#{name} no longer exists"})}
    end
  end

  defp root, do: Application.fetch_env!(:kafun, :root)

  defp humanize_bytes(n) when n < 1024, do: "#{n} B"
  defp humanize_bytes(n) when n < 1024 * 1024, do: "#{Float.round(n / 1024, 1)} KiB"
  defp humanize_bytes(n) when n < 1024 * 1024 * 1024, do: "#{Float.round(n / (1024 * 1024), 1)} MiB"
  defp humanize_bytes(n), do: "#{Float.round(n / (1024 * 1024 * 1024), 2)} GiB"

  defp format_date(unix), do: unix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M")

  @impl true
  def render(assigns) do
    ~H"""
    <h1>Buckets</h1>

    <%= if @notice do %>
      <div class={"flash flash-#{elem(@notice, 0)}"}>{elem(@notice, 1)}</div>
    <% end %>

    <form phx-submit="create" class="row" style="margin-bottom: 1.5rem;">
      <input type="text" name="name" value={@new_name}
             placeholder="new-bucket-name" autocomplete="off"
             style="max-width: 320px;" />
      <button type="submit" class="btn btn-primary">Create</button>
    </form>

    <%= if @buckets == [] do %>
      <div class="empty">No buckets yet. Create one above.</div>
    <% else %>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Created</th>
            <th class="num">Objects</th>
            <th class="num">Size</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <%= for b <- @buckets do %>
            <tr>
              <td>
                <.link navigate={~p"/buckets/#{b.name}"}>{b.name}</.link>
                <%= if b.public_read do %>
                  <span class="pill pill-public" title="Public read access enabled">🌐 public</span>
                <% end %>
              </td>
              <td>{format_date(b.created_at)}</td>
              <td class="num">{b.object_count}</td>
              <td class="num">{humanize_bytes(b.total_bytes)}</td>
              <td>
                <button class="btn btn-danger" phx-click="delete"
                        phx-value-name={b.name}
                        data-confirm={"Delete bucket #{b.name}? This requires it to be empty."}>
                  Delete
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
