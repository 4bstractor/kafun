defmodule Kafun.Admin.StatusLive do
  @moduledoc """
  GC status + telemetry counters. Subscribes to the kafun telemetry events
  on mount (one handler per LV process, scoped by the socket id) and
  detaches on terminate.
  """

  use Phoenix.LiveView

  use Phoenix.VerifiedRoutes,
    endpoint: Kafun.Admin.Endpoint,
    router: Kafun.Admin.Router,
    statics: ~w(assets favicon.ico favicon.png)

  @events [
    [:kafun, :put, :stop],
    [:kafun, :get, :stop],
    [:kafun, :delete, :stop],
    [:kafun, :copy, :stop],
    [:kafun, :delete_objects, :stop],
    [:kafun, :multipart, :initiate],
    [:kafun, :multipart, :upload_part],
    [:kafun, :multipart, :complete],
    [:kafun, :multipart, :abort],
    [:kafun, :multipart, :upload_part_copy],
    [:kafun, :gc, :run]
  ]

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      attach_telemetry(socket.id, self())
      Process.send_after(self(), :tick, 1_000)
    end

    {:ok,
     assign(socket,
       gc: Kafun.GC.status(),
       counters: Map.new(@events, fn ev -> {ev, 0} end),
       bytes_in: 0,
       bytes_out: 0,
       since: System.system_time(:second),
       notice: nil
     )}
  end

  @impl true
  def terminate(_reason, socket) do
    :telemetry.detach(handler_id(socket.id))
    :ok
  end

  @impl true
  def handle_info(:tick, socket) do
    Process.send_after(self(), :tick, 1_000)
    {:noreply, assign(socket, gc: Kafun.GC.status())}
  end

  def handle_info({:kafun_event, event, measurements, _meta}, socket) do
    counters = Map.update(socket.assigns.counters, event, 1, &(&1 + 1))

    bytes_in =
      case event do
        [:kafun, :put, :stop] -> socket.assigns.bytes_in + Map.get(measurements, :size, 0)
        [:kafun, :copy, :stop] -> socket.assigns.bytes_in + Map.get(measurements, :size, 0)
        _ -> socket.assigns.bytes_in
      end

    bytes_out =
      case event do
        [:kafun, :get, :stop] -> socket.assigns.bytes_out + Map.get(measurements, :size, 0)
        _ -> socket.assigns.bytes_out
      end

    {:noreply, assign(socket, counters: counters, bytes_in: bytes_in, bytes_out: bytes_out)}
  end

  @impl true
  def handle_event("run_gc", _params, socket) do
    _result = Kafun.GC.run_now()
    {:noreply, assign(socket, gc: Kafun.GC.status(), notice: {:info, "GC sweep complete"})}
  end

  ## Helpers

  defp attach_telemetry(socket_id, lv_pid) do
    :telemetry.attach_many(
      handler_id(socket_id),
      @events,
      fn name, measurements, metadata, _config ->
        send(lv_pid, {:kafun_event, name, measurements, metadata})
      end,
      nil
    )
  end

  defp handler_id(socket_id), do: "kafun-status-#{socket_id}"

  defp humanize_bytes(n) when n < 1024, do: "#{n} B"
  defp humanize_bytes(n) when n < 1024 * 1024, do: "#{Float.round(n / 1024, 1)} KiB"
  defp humanize_bytes(n) when n < 1024 * 1024 * 1024, do: "#{Float.round(n / (1024 * 1024), 1)} MiB"
  defp humanize_bytes(n), do: "#{Float.round(n / (1024 * 1024 * 1024), 2)} GiB"

  defp format_date(nil), do: "—"
  defp format_date(unix), do: unix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M:%S")

  defp ago(nil), do: "—"

  defp ago(unix) do
    diff = max(System.system_time(:second) - unix, 0)

    cond do
      diff < 60 -> "#{diff}s ago"
      diff < 3600 -> "#{div(diff, 60)}m ago"
      diff < 86_400 -> "#{div(diff, 3600)}h ago"
      true -> "#{div(diff, 86_400)}d ago"
    end
  end

  defp eta(nil), do: "disabled"

  defp eta(unix) do
    diff = unix - System.system_time(:second)

    cond do
      diff <= 0 -> "any moment"
      diff < 60 -> "in #{diff}s"
      diff < 3600 -> "in #{div(diff, 60)}m #{rem(diff, 60)}s"
      true -> "in #{div(diff, 3600)}h #{div(rem(diff, 3600), 60)}m"
    end
  end

  defp event_label([:kafun, op, :stop]), do: "#{op}"
  defp event_label([:kafun, :multipart, sub]), do: "multipart.#{sub}"
  defp event_label([:kafun, :gc, :run]), do: "gc.run"
  defp event_label(other), do: Enum.join(other, ".")

  @impl true
  def render(assigns) do
    ~H"""
    <h1>Status</h1>

    <%= if @notice do %>
      <div class={"flash flash-#{elem(@notice, 0)}"}>{elem(@notice, 1)}</div>
    <% end %>

    <h2>Garbage collection</h2>
    <dl class="meta-grid">
      <dt>Interval</dt>
      <dd>
        {if @gc.interval_ms == 0, do: "disabled (KAFUN_GC_INTERVAL_SEC=0)",
              else: "#{div(@gc.interval_ms, 1000)}s"}
      </dd>
      <dt>Last run</dt>
      <dd>{format_date(@gc.last_run_at)} ({ago(@gc.last_run_at)})</dd>
      <dt>Next tick</dt>
      <dd>{eta(@gc.next_tick_at)}</dd>
      <%= if @gc.last_result do %>
        <dt>Abandoned uploads</dt><dd>{@gc.last_result.abandoned}</dd>
        <dt>Orphan part dirs</dt><dd>{@gc.last_result.orphans}</dd>
        <dt>Orphan blobs</dt><dd>{@gc.last_result.orphan_blobs}</dd>
        <dt>Sweep duration</dt><dd>{div(@gc.last_result.duration_us, 1000)} ms</dd>
      <% end %>
    </dl>
    <div class="row" style="margin-top: 0.75rem;">
      <button class="btn" phx-click="run_gc">Run GC now</button>
    </div>

    <h2>Telemetry counters</h2>
    <p style="color: #8b949e;">
      Counts since this page loaded ({format_date(@since)}).
    </p>
    <table>
      <thead>
        <tr>
          <th>Event</th>
          <th class="num">Count</th>
        </tr>
      </thead>
      <tbody>
        <%= for {ev, n} <- Enum.sort_by(@counters, fn {ev, _} -> event_label(ev) end) do %>
          <tr>
            <td><code>{event_label(ev)}</code></td>
            <td class="num">{n}</td>
          </tr>
        <% end %>
        <tr>
          <td>bytes ingressed (PUT + Copy)</td>
          <td class="num">{humanize_bytes(@bytes_in)}</td>
        </tr>
        <tr>
          <td>bytes egressed (GET)</td>
          <td class="num">{humanize_bytes(@bytes_out)}</td>
        </tr>
      </tbody>
    </table>
    """
  end
end
