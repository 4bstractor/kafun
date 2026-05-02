defmodule Kafun.Admin.Router do
  use Phoenix.Router, helpers: false

  import Phoenix.Controller
  import Phoenix.LiveView.Router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug Kafun.Admin.Auth
    plug :put_root_layout, html: {Kafun.Admin.Layouts, :root}
  end

  scope "/", Kafun.Admin do
    pipe_through :browser

    live "/", BucketsLive, :index
    live "/buckets", BucketsLive, :index
    live "/buckets/:bucket", BucketLive, :index
    live "/buckets/:bucket/*key_parts", ObjectLive, :show
    live "/uploads", UploadsLive, :index
    live "/status", StatusLive, :index
  end
end
