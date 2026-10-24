# fh-fc26-draw

Live draws and knockout brackets (4, 8 or 16 players), hosted on Railway.

- `server.js`: Node server, Postgres (`DATABASE_URL`), live updates over server-sent events.
- `public/`: home page (tournament list, create form) and `tournament.html` (draw + bracket).
- `ADMIN_KEY` is the master key: open `/?admin=<key>` to create tournaments. Each tournament also gets its own organizer key.
- `index.html` at the root only redirects the old GitHub Pages link to Railway.

Run locally: `DATABASE_URL=postgres://… ADMIN_KEY=… npm start`
