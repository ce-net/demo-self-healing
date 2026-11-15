# Putting the page on the public web

The front listens on loopback (127.0.0.1:8974). Two staged steps put it on the web; neither has run yet.

1. Gate route, no sign-in (apps.ce-net.com/self-healing/ -> the front). One entry in the gate's no-session table
   (`CAP_PATHS` in the gate's door/gate.py), then the normal pinned gate deploy on every gate host:

       "self-healing": ("127.0.0.1", 8974),

   The front rate-limits by `X-Ce-Client-Ip`, which the gate sets from the visitor's address.

2. ce-net.com/demos/self-healing/ (Cloudflare Pages): the files in `web/` copied to the site's `demos/self-healing/`.
   On ce-net.com the page calls `https://apps.ce-net.com/self-healing/api/` (CORS allowed for https://ce-net.com by
   the front's `--origin`). Deploy with the site's usual Pages command:

       npx wrangler pages deploy . --project-name ce-net-com --branch main
