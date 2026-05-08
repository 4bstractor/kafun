defmodule Kafun.Admin.KeysLive do
  @moduledoc """
  Access-key administration. List active and revoked keys, generate new
  ones (the secret is shown ONCE post-generation), revoke, edit
  descriptions, rotate secrets.

  Generated keys land in the index with no grants. The operator visits
  the bucket browser to attach per-bucket grants (PR 4) or assigns a
  global `*` admin grant via the per-key actions on this page.
  """

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico favicon.png)

  alias Kafun.Index

  @impl true
  def mount(_params, _session, socket) do
    {:ok,
     assign(socket,
       keys: load_keys(),
       new_description: "",
       editing_description: nil,
       generated: nil,
       notice: nil
     )}
  end

  @impl true
  def handle_event("generate", %{"description" => description}, socket) do
    description = String.trim(description)
    id = generate_key_id()
    secret = generate_secret()

    :ok = Index.create_access_key(id, secret, description)

    {:noreply,
     assign(socket,
       keys: load_keys(),
       new_description: "",
       generated: %{id: id, secret: secret, description: description},
       notice: nil
     )}
  end

  def handle_event("dismiss_generated", _params, socket) do
    {:noreply, assign(socket, generated: nil)}
  end

  def handle_event("revoke", %{"id" => id}, socket) do
    case Index.revoke_access_key(id) do
      :ok ->
        {:noreply,
         assign(socket, keys: load_keys(), notice: {:info, "revoked #{mask_id(id)}"})}

      :not_found ->
        {:noreply, assign(socket, notice: {:error, "key not found"})}
    end
  end

  def handle_event("edit_description", %{"id" => id}, socket) do
    {:noreply, assign(socket, editing_description: id)}
  end

  def handle_event("cancel_edit", _params, socket) do
    {:noreply, assign(socket, editing_description: nil)}
  end

  def handle_event("save_description", %{"key_id" => id, "description" => description}, socket) do
    description = String.trim(description)

    case Index.set_access_key_description(id, description) do
      :ok ->
        {:noreply,
         assign(socket,
           keys: load_keys(),
           editing_description: nil,
           notice: {:info, "description updated"}
         )}

      :not_found ->
        {:noreply, assign(socket, notice: {:error, "key not found"})}
    end
  end

  def handle_event("rotate_secret", %{"id" => id}, socket) do
    new_secret = generate_secret()

    case Index.set_access_key_secret(id, new_secret) do
      :ok ->
        # Pull the row so we can show the description alongside the
        # new secret in the same "save this now" panel.
        {:ok, key} = Index.get_access_key(id)

        {:noreply,
         assign(socket,
           keys: load_keys(),
           generated: %{id: id, secret: new_secret, description: key.description},
           notice: nil
         )}

      :not_found ->
        {:noreply, assign(socket, notice: {:error, "key not found"})}
    end
  end

  ## Helpers

  defp load_keys do
    Index.list_access_keys()
    |> Enum.sort_by(fn k ->
      # Active first (alphabetical by id), then revoked.
      {k.status != :active, k.id}
    end)
  end

  defp generate_key_id do
    alphabet = ~c"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    for _ <- 1..20, into: "" do
      <<Enum.random(alphabet)>>
    end
  end

  defp generate_secret do
    :crypto.strong_rand_bytes(30) |> Base.url_encode64(padding: false) |> String.slice(0..39)
  end

  defp mask_id(id) when byte_size(id) >= 8 do
    String.slice(id, 0..3) <> "…" <> String.slice(id, -4..-1//1)
  end

  defp mask_id(id), do: id

  defp format_date(nil), do: "—"
  defp format_date(unix), do: unix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M")

  defp format_status(:active), do: "active"
  defp format_status(:revoked), do: "revoked"

  @impl true
  def render(assigns) do
    ~H"""
    <h1>Access keys</h1>

    <%= if @notice do %>
      <div class={"flash flash-#{elem(@notice, 0)}"}>{elem(@notice, 1)}</div>
    <% end %>

    <%= if @generated do %>
      <div class="generated-key">
        <h2>New secret — save this now</h2>
        <p class="generated-warn">
          You will not see this secret again. Copy it into wherever your
          client is configured (boto3 config, env file, etc.) before
          dismissing this panel.
        </p>
        <dl class="meta-grid">
          <dt>Access key ID</dt>
          <dd><code>{@generated.id}</code></dd>
          <dt>Secret</dt>
          <dd><code>{@generated.secret}</code></dd>
          <%= if @generated.description != "" do %>
            <dt>Description</dt>
            <dd>{@generated.description}</dd>
          <% end %>
        </dl>
        <button class="btn btn-primary" phx-click="dismiss_generated">I've saved it</button>
      </div>
    <% end %>

    <h2>Generate</h2>
    <form phx-submit="generate" class="row" style="margin-bottom: 1.5rem;">
      <input type="text" name="description" value={@new_description}
             placeholder="(optional) description — e.g. 'imouto pipeline'"
             style="max-width: 480px;" />
      <button type="submit" class="btn btn-primary">Generate new key</button>
    </form>

    <%= if @keys == [] do %>
      <div class="empty">No access keys yet. Generate one above.</div>
    <% else %>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Description</th>
            <th>Status</th>
            <th>Created</th>
            <th>Last used</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <%= for key <- @keys do %>
            <tr class={if key.status == :revoked, do: "row-muted", else: ""}>
              <td><code>{mask_id(key.id)}</code></td>
              <td>
                <%= if @editing_description == key.id do %>
                  <form phx-submit="save_description" class="row">
                    <input type="hidden" name="key_id" value={key.id} />
                    <input type="text" name="description" value={key.description}
                           autofocus style="max-width: 360px;" />
                    <button type="submit" class="btn">Save</button>
                    <button type="button" class="btn" phx-click="cancel_edit">Cancel</button>
                  </form>
                <% else %>
                  <span>{if key.description == "", do: "—", else: key.description}</span>
                  <%= if key.status == :active do %>
                    <button class="btn btn-link" phx-click="edit_description"
                            phx-value-id={key.id} title="Edit description">✎</button>
                  <% end %>
                <% end %>
              </td>
              <td>
                <span class={"pill pill-#{format_status(key.status)}"}>
                  {format_status(key.status)}
                </span>
              </td>
              <td>{format_date(key.created_at)}</td>
              <td>{format_date(key.last_used_at)}</td>
              <td>
                <%= if key.status == :active do %>
                  <button class="btn" phx-click="rotate_secret" phx-value-id={key.id}
                          data-confirm={"Rotate the secret for #{mask_id(key.id)}? The old secret stops working immediately and the new one is shown once."}>
                    Rotate secret
                  </button>
                  <button class="btn btn-danger" phx-click="revoke" phx-value-id={key.id}
                          data-confirm={"Revoke #{mask_id(key.id)}? Clients using this key will start failing immediately. Revoked keys can't be reactivated."}>
                    Revoke
                  </button>
                <% end %>
              </td>
            </tr>
          <% end %>
        </tbody>
      </table>
    <% end %>
    """
  end
end
