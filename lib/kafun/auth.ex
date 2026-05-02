defmodule Kafun.Auth do
  @moduledoc """
  Access-key gating. Parses the SigV4 `Authorization` header for the access key
  and checks it against `KAFUN_KEYS`. The signature itself is **not** verified —
  this service assumes a trusted network. If `KAFUN_KEYS` is empty, auth is off.
  """

  @sigv4 ~r/Credential=([^\/,\s]+)\//
  @qs_credential ~r/(?:^|&)X-Amz-Credential=([^\/&]+)/

  @doc "Returns `{:ok, key}` if a key was found, `:error` otherwise."
  def access_key(conn) do
    with :error <- from_header(conn),
         :error <- from_query(conn) do
      :error
    end
  end

  defp from_header(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      [auth | _] ->
        case Regex.run(@sigv4, auth) do
          [_, key] -> {:ok, key}
          _ -> :error
        end

      [] ->
        :error
    end
  end

  defp from_query(conn) do
    qs = conn.query_string || ""

    case Regex.run(@qs_credential, qs) do
      [_, key] -> {:ok, URI.decode(key)}
      _ -> :error
    end
  end

  @doc "Returns true if `key` is in the allowed set, or if the allowed set is empty."
  def allowed?(key) do
    keys = Application.fetch_env!(:kafun, :allowed_keys)
    MapSet.size(keys) == 0 or MapSet.member?(keys, key)
  end

  @doc "Returns true if auth is disabled altogether."
  def disabled? do
    MapSet.size(Application.fetch_env!(:kafun, :allowed_keys)) == 0
  end
end
