defmodule Kafun.Application do
  @moduledoc false
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    children =
      if Application.get_env(:kafun, :start_children?, true) do
        root = Application.fetch_env!(:kafun, :root)
        db_path = Application.fetch_env!(:kafun, :db_path)
        host = Application.fetch_env!(:kafun, :host)
        port = Application.fetch_env!(:kafun, :port)

        File.mkdir_p!(root)
        Logger.info("kafun starting: root=#{root} db=#{db_path} bind=#{host}:#{port}")

        [
          {Kafun.Index, db_path: db_path},
          {Bandit, plug: Kafun.Router, port: port, ip: parse_ip(host)}
        ]
      else
        []
      end

    Supervisor.start_link(children, strategy: :one_for_one, name: Kafun.Supervisor)
  end

  defp parse_ip("0.0.0.0"), do: {0, 0, 0, 0}
  defp parse_ip("127.0.0.1"), do: {127, 0, 0, 1}
  defp parse_ip("::"), do: {0, 0, 0, 0, 0, 0, 0, 0}

  defp parse_ip(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, ip} -> ip
      {:error, _} -> raise "invalid KAFUN_HOST: #{host}"
    end
  end
end
