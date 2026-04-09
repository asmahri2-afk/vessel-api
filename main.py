// ═══════════════════════════════════════════════════════════════════════════════
// Cloudflare Worker: Vessel Manager
// Handles all reads + add/remove vessels + triggers GitHub workflow
// ═══════════════════════════════════════════════════════════════════════════════

// ── In-memory rate limiter for auth endpoints ────────────────────────────────
// Limits per IP: 10 requests per 15 minutes on auth routes.
// Uses a Map that auto-clears old entries to avoid memory growth.
const _rateLimitMap = new Map(); // key: `${ip}:${route}` → { count, resetAt }
const RATE_LIMIT_MAX      = 10;
const RATE_LIMIT_WINDOW   = 15 * 60 * 1000; // 15 minutes

function checkRateLimit(ip, route) {
    const key = `${ip}:${route}`;
    const now = Date.now();
    const entry = _rateLimitMap.get(key);
    if (!entry || now > entry.resetAt) {
        _rateLimitMap.set(key, { count: 1, resetAt: now + RATE_LIMIT_WINDOW });
        return false; // not limited
    }
    if (entry.count >= RATE_LIMIT_MAX) return true; // limited
    entry.count++;
    return false;
}

export default {
  async fetch(request, env) {
    // CORS headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    };

    // Handle preflight requests
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // Apply rate limiting to all auth endpoints
    const authPaths = ['/auth/login', '/auth/register', '/auth/delete'];
    if (authPaths.includes(path)) {
      const ip = request.headers.get('CF-Connecting-IP') || request.headers.get('X-Forwarded-For') || 'unknown';
      if (checkRateLimit(ip, path)) {
        return jsonResponse({ error: 'Too many requests — try again in 15 minutes' }, 429, corsHeaders);
      }
    }

    try {

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Load all data
      // Accepts optional Authorization: Bearer <user_token> header.
      // With token → returns that user's personal fleet.
      // Without    → returns public fleet (user_id IS NULL).
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/data/load' && request.method === 'GET') {
        // Resolve user from token if present
        let userId = null;
        const authHeader = request.headers.get('Authorization');
        if (authHeader && authHeader.startsWith('Bearer ')) {
          const token = authHeader.slice(7);
          try {
            const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
              headers: {
                'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                'Authorization': `Bearer ${token}`,
              },
            });
            if (userRes.ok) {
              const user = await userRes.json();
              userId = user.id || null;
            }
          } catch (_) {}
        }

        const imoQuery = userId
          ? `select=imo&user_id=eq.${userId}`
          : 'select=imo&user_id=is.null';

        const [trackedRows, allVessels, cache, ports] = await Promise.all([
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, 'tracked_imos', imoQuery, 'GET'),
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, 'vessels', 'select=*', 'GET'),
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, 'static_vessel_cache', 'select=*', 'GET'),
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, 'ports', 'select=name,lat,lon,anchorage_depth,cargo_pier_depth', 'GET'),
        ]);

        // Filter vessels to only those being tracked (avoids sending full table to browser)
        const trackedImos = new Set(trackedRows.map(r => String(r.imo)));
        const vessels = allVessels.filter(v => trackedImos.has(String(v.imo)));

        return jsonResponse({ tracked: trackedRows, vessels, cache, ports }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Load sanctions list (replaces sbFetch in loadSanctionsLists())
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/data/sanctions' && request.method === 'GET') {
        const data = await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'sanctioned_imos',
          'select=imo,name,lists,program&limit=10000',
          'GET'
        );

        return jsonResponse(data, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Register new user
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/auth/register' && request.method === 'POST') {
        const body = await request.json();
        const { username, pin } = body;

        if (!username || username.length < 3 || !/^[a-zA-Z0-9_]+$/.test(username)) {
          return jsonResponse({ error: 'Invalid username (min 3 chars, alphanumeric/underscore)' }, 400, corsHeaders);
        }
        if (!pin || !/^\d{4,6}$/.test(String(pin))) {
          return jsonResponse({ error: 'Invalid PIN (4-6 digits)' }, 400, corsHeaders);
        }

        // Check if username already taken
        const existing = await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles',
          `username=eq.${encodeURIComponent(username)}&select=id`,
          'GET'
        );

        if (existing.length > 0) {
          return jsonResponse({ error: 'Username already taken' }, 400, corsHeaders);
        }

        // Create Supabase auth user via admin API
        const email = `${username.toLowerCase()}@vt.local`;
        const createUserRes = await fetch(`${env.SUPABASE_URL}/auth/v1/admin/users`, {
          method: 'POST',
          headers: {
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            email,
            password: String(pin),
            email_confirm: true,
            email_confirmed_at: new Date().toISOString(), // required by newer Supabase versions
          }),
        });

        if (!createUserRes.ok) {
          const errText = await createUserRes.text();
          throw new Error(`Auth user creation failed: ${errText}`);
        }

        const userData = await createUserRes.json();
        const userId = userData.id;

        // Create user_profiles row
        await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles',
          null,
          'POST',
          { id: userId, username }
        );

        return jsonResponse({ success: true, user_id: userId }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Login
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/auth/login' && request.method === 'POST') {
        const body = await request.json();
        const { username, pin } = body;

        if (!username || !pin) {
          return jsonResponse({ error: 'Username and PIN required' }, 400, corsHeaders);
        }

        const email = `${username.toLowerCase()}@vt.local`;

        const tokenRes = await fetch(`${env.SUPABASE_URL}/auth/v1/token?grant_type=password`, {
          method: 'POST',
          headers: {
            // grant_type=password requires the anon/publishable key, not service role key
            'apikey': env.SUPABASE_ANON_KEY || env.SUPABASE_SERVICE_ROLE_KEY,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ email, password: String(pin) }),
        });

        if (!tokenRes.ok) {
          const errBody = await tokenRes.json().catch(() => ({}));
          const reason = errBody.error_description || errBody.msg || errBody.error || 'Invalid username or PIN';
          return jsonResponse({ error: reason }, 401, corsHeaders);
        }

        const tokenData = await tokenRes.json();

        return jsonResponse({
          success: true,
          access_token: tokenData.access_token,
          user_id: tokenData.user?.id,
          username,
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Delete Account
      // Removes: tracked_imos (user), user_profiles row, Supabase auth user
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/auth/delete' && request.method === 'POST') {
        const body = await request.json();
        const { user_token, pin } = body;

        if (!user_token) {
          return jsonResponse({ error: 'user_token required' }, 400, corsHeaders);
        }

        // Validate token and get user
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: {
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${user_token}`,
          },
        });

        if (!userRes.ok) {
          return jsonResponse({ error: 'Invalid or expired token' }, 401, corsHeaders);
        }

        const user = await userRes.json();
        const userId = user.id;
        const email = user.email;

        // Re-verify PIN before deletion — re-authenticate with password
        if (pin) {
          const verifyRes = await fetch(`${env.SUPABASE_URL}/auth/v1/token?grant_type=password`, {
            method: 'POST',
            headers: {
              'apikey': env.SUPABASE_ANON_KEY || env.SUPABASE_SERVICE_ROLE_KEY,
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({ email, password: String(pin) }),
          });
          if (!verifyRes.ok) {
            return jsonResponse({ error: 'Incorrect PIN — account not deleted' }, 401, corsHeaders);
          }
        }

        // 1. Delete all tracked IMOs for this user
        await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos',
          `user_id=eq.${userId}`,
          'DELETE'
        );

        // 2. Delete user_profiles row
        await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles',
          `id=eq.${userId}`,
          'DELETE'
        );

        // 3. Delete Supabase Auth user (admin API)
        const deleteRes = await fetch(`${env.SUPABASE_URL}/auth/v1/admin/users/${userId}`, {
          method: 'DELETE',
          headers: {
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
          },
        });

        if (!deleteRes.ok) {
          const errText = await deleteRes.text();
          throw new Error(`Auth user deletion failed: ${errText}`);
        }

        return jsonResponse({ success: true, message: 'Account deleted successfully' }, 200, corsHeaders);
      }
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/user/settings' && request.method === 'POST') {
        const body = await request.json();
        const { user_token, callmebot_phone, callmebot_apikey, callmebot_enabled } = body;

        if (!user_token) {
          return jsonResponse({ error: 'user_token required' }, 400, corsHeaders);
        }

        // Validate JWT by calling Supabase /auth/v1/user
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: {
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${user_token}`,
          },
        });

        if (!userRes.ok) {
          return jsonResponse({ error: 'Invalid or expired token' }, 401, corsHeaders);
        }

        const user = await userRes.json();
        const userId = user.id;

        // Update user_profiles
        await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles',
          `id=eq.${userId}`,
          'PATCH',
          {
            callmebot_phone: callmebot_phone || null,
            callmebot_apikey: callmebot_apikey || null,
            callmebot_enabled: !!callmebot_enabled,
          }
        );

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Admin dashboard data — only accessible by username 'asmahri'
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/admin/data' && request.method === 'GET') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }
        const token = authHeader.slice(7);

        // Validate token
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: {
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${token}`,
          },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const authUser = await userRes.json();

        // Check username is asmahri — anyone else gets 403
        const profileCheck = await supabaseFetch(
          env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles', `id=eq.${authUser.id}&select=username`, 'GET'
        );
        if (!profileCheck.length || profileCheck[0].username !== 'asmahri') {
          return jsonResponse({ error: 'Forbidden' }, 403, corsHeaders);
        }

        // Fetch all data in parallel
        const [users, trackedImos, vessels] = await Promise.all([
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'user_profiles', 'select=id,username,callmebot_enabled,callmebot_phone,callmebot_apikey', 'GET'),
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'tracked_imos', 'select=imo,user_id', 'GET'),
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'vessels', 'select=imo,name,sog,destination,last_alert_utc,nearest_port,nearest_distance_nm,eta_text,updated_at', 'GET'),
        ]);

        // Build vessel lookup map
        const vesselMap = {};
        vessels.forEach(v => { vesselMap[String(v.imo)] = v; });

        // Build per-user fleet map
        const userFleets = {};
        trackedImos.forEach(row => {
          const uid = row.user_id || '__public__';
          if (!userFleets[uid]) userFleets[uid] = [];
          userFleets[uid].push(String(row.imo));
        });

        // Enrich users with their vessel data
        const enrichedUsers = users.map(u => ({
          id: u.id,
          username: u.username,
          callmebot_enabled: u.callmebot_enabled,
          callmebot_phone: u.callmebot_phone || '',
          callmebot_apikey: u.callmebot_apikey || '',
          vessels: (userFleets[u.id] || []).map(imo => ({
            imo,
            ...(vesselMap[imo] || { name: `IMO ${imo}` })
          }))
        }));

        // Public fleet (user_id IS NULL)
        const publicFleet = (userFleets['__public__'] || []).map(imo => ({
          imo,
          ...(vesselMap[imo] || { name: `IMO ${imo}` })
        }));

        return jsonResponse({
          users: enrichedUsers,
          public_fleet: publicFleet,
          total_users: enrichedUsers.length,
          total_vessels: vessels.length,
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Admin — Update user CallMeBot settings
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/admin/user/update' && request.method === 'POST') {
        const body = await request.json();
        const { admin_token, user_id, callmebot_phone, callmebot_apikey, callmebot_enabled } = body;

        if (!admin_token || !user_id) return jsonResponse({ error: 'admin_token and user_id required' }, 400, corsHeaders);

        // Verify admin
        const adminCheck = await verifyAdmin(env, admin_token);
        if (!adminCheck.ok) return jsonResponse({ error: adminCheck.error }, adminCheck.status, corsHeaders);

        await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles', `id=eq.${user_id}`, 'PATCH',
          {
            callmebot_phone: callmebot_phone || null,
            callmebot_apikey: callmebot_apikey || null,
            callmebot_enabled: !!callmebot_enabled,
          }
        );

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Admin — Reset user PIN
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/admin/user/pin' && request.method === 'POST') {
        const body = await request.json();
        const { admin_token, user_id, new_pin } = body;

        if (!admin_token || !user_id || !new_pin) return jsonResponse({ error: 'admin_token, user_id and new_pin required' }, 400, corsHeaders);
        if (!/^\d{4,6}$/.test(String(new_pin))) return jsonResponse({ error: 'PIN must be 4-6 digits' }, 400, corsHeaders);

        // Verify admin
        const adminCheck = await verifyAdmin(env, admin_token);
        if (!adminCheck.ok) return jsonResponse({ error: adminCheck.error }, adminCheck.status, corsHeaders);

        const res = await fetch(`${env.SUPABASE_URL}/auth/v1/admin/users/${user_id}`, {
          method: 'PUT',
          headers: {
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ password: String(new_pin) }),
        });

        if (!res.ok) {
          const err = await res.text();
          throw new Error(`PIN reset failed: ${err}`);
        }

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Admin — Add vessel to user's fleet
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/admin/fleet/add' && request.method === 'POST') {
        const body = await request.json();
        const { admin_token, user_id, imo } = body;

        if (!admin_token || !user_id || !imo) return jsonResponse({ error: 'admin_token, user_id and imo required' }, 400, corsHeaders);

        const adminCheck = await verifyAdmin(env, admin_token);
        if (!adminCheck.ok) return jsonResponse({ error: adminCheck.error }, adminCheck.status, corsHeaders);

        const cleanImo = String(imo).replace(/[^\d]/g, '');
        if (cleanImo.length !== 7) return jsonResponse({ error: 'Invalid IMO' }, 400, corsHeaders);

        // Check not already tracked
        const existing = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos', `imo=eq.${cleanImo}&user_id=eq.${user_id}&select=imo`, 'GET');
        if (existing.length > 0) return jsonResponse({ error: 'Already tracked' }, 400, corsHeaders);

        await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos', null, 'POST', { imo: cleanImo, user_id });

        return jsonResponse({ success: true, imo: cleanImo }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Admin — Remove vessel from user's fleet
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/admin/fleet/remove' && request.method === 'POST') {
        const body = await request.json();
        const { admin_token, user_id, imo } = body;

        if (!admin_token || !user_id || !imo) return jsonResponse({ error: 'admin_token, user_id and imo required' }, 400, corsHeaders);

        const adminCheck = await verifyAdmin(env, admin_token);
        if (!adminCheck.ok) return jsonResponse({ error: adminCheck.error }, adminCheck.status, corsHeaders);

        const cleanImo = String(imo).replace(/[^\d]/g, '');

        await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos', `imo=eq.${cleanImo}&user_id=eq.${user_id}`, 'DELETE');

        // Clean up vessels table if no other trackers
        const remaining = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos', `imo=eq.${cleanImo}&select=imo`, 'GET');
        if (remaining.length === 0) {
          await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'vessels', `imo=eq.${cleanImo}`, 'DELETE');
        }

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Add Vessel
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/vessel/add' && request.method === 'POST') {
        const body = await request.json();
        const { imo, secret, user_token } = body;

        // Determine auth mode and resolve user_id
        let userId = null;

        if (user_token) {
          // Personal fleet — validate JWT
          const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
            headers: {
              'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
              'Authorization': `Bearer ${user_token}`,
            },
          });
          if (!userRes.ok) {
            return jsonResponse({ error: 'Invalid or expired token' }, 401, corsHeaders);
          }
          const user = await userRes.json();
          userId = user.id;
        } else if (secret === env.API_SECRET) {
          // Public fleet — userId stays null
          userId = null;
        } else {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        // Validate IMO
        const cleanImo = String(imo).replace(/[^\d]/g, '');
        if (cleanImo.length !== 7) {
          return jsonResponse({ error: 'Invalid IMO number' }, 400, corsHeaders);
        }

        // Check if already tracked (for this user scope)
        const existingQuery = userId
          ? `imo=eq.${cleanImo}&user_id=eq.${userId}`
          : `imo=eq.${cleanImo}&user_id=is.null`;

        const existing = await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos',
          existingQuery,
          'GET'
        );

        if (existing.length > 0) {
          return jsonResponse({ error: 'Vessel already tracked', imo: cleanImo }, 400, corsHeaders);
        }

        // Add to tracked_imos table
        const insertRow = userId
          ? { imo: cleanImo, user_id: userId }
          : { imo: cleanImo };

        await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos',
          null,
          'POST',
          insertRow
        );

        // ─────────────────────────────────────────────────────────────────────
        // FIX 1: Always trigger workflow when a vessel is added to tracking.
        // Removed the vesselExists check — even if a row exists the data may be
        // stale and needs a fresh scrape now that someone is actively tracking it.
        // ─────────────────────────────────────────────────────────────────────
        const workflowTriggered = await triggerGitHubWorkflow(
          env.GITHUB_TOKEN,
          env.GITHUB_REPO,
          env.GITHUB_BRANCH || 'main'
        );

        return jsonResponse({
          success: true,
          imo: cleanImo,
          message: 'Vessel added successfully',
          workflowTriggered
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Remove Vessel
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/vessel/remove' && request.method === 'POST') {
        const body = await request.json();
        const { imo, secret, user_token } = body;

        // Determine auth mode
        let userId = null;
        let isPublicFleet = false;

        if (user_token) {
          // Personal fleet — validate JWT
          const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
            headers: {
              'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
              'Authorization': `Bearer ${user_token}`,
            },
          });
          if (!userRes.ok) {
            return jsonResponse({ error: 'Invalid or expired token' }, 401, corsHeaders);
          }
          const user = await userRes.json();
          userId = user.id;
        } else if (secret === env.API_SECRET) {
          isPublicFleet = true;
        } else {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        // Validate IMO
        const cleanImo = String(imo).replace(/[^\d]/g, '');
        if (cleanImo.length !== 7) {
          return jsonResponse({ error: 'Invalid IMO number' }, 400, corsHeaders);
        }

        if (userId) {
          // Personal fleet: remove from tracked_imos for this user
          await supabaseFetch(
            env.SUPABASE_URL,
            env.SUPABASE_SERVICE_ROLE_KEY,
            'tracked_imos',
            `imo=eq.${cleanImo}&user_id=eq.${userId}`,
            'DELETE'
          );

          // Check if any other user (or public fleet) still tracks this IMO.
          // If nobody else tracks it, clean up vessels + static_vessel_cache so
          // the next person to add it gets a fresh "First tracking detected" alert.
          const remainingTrackers = await supabaseFetch(
            env.SUPABASE_URL,
            env.SUPABASE_SERVICE_ROLE_KEY,
            'tracked_imos',
            `imo=eq.${cleanImo}&select=imo`,
            'GET'
          );

          if (remainingTrackers.length === 0) {
            // Delete live AIS state so scraper treats next add as "first tracking"
            // and fires the WhatsApp alert correctly.
            // static_vessel_cache is intentionally kept — it stores vessel metadata
            // (name, flag, dimensions) used for instant preview when re-adding the IMO.
            await supabaseFetch(
              env.SUPABASE_URL,
              env.SUPABASE_SERVICE_ROLE_KEY,
              'vessels',
              `imo=eq.${cleanImo}`,
              'DELETE'
            );
          }
        } else {
          // Public fleet: remove from tracked_imos, vessels, and static_vessel_cache
          await supabaseFetch(
            env.SUPABASE_URL,
            env.SUPABASE_SERVICE_ROLE_KEY,
            'tracked_imos',
            `imo=eq.${cleanImo}&user_id=is.null`,
            'DELETE'
          );

          // Remove from vessels table
          await supabaseFetch(
            env.SUPABASE_URL,
            env.SUPABASE_SERVICE_ROLE_KEY,
            'vessels',
            `imo=eq.${cleanImo}`,
            'DELETE'
          );

          // Remove from static_vessel_cache
          await supabaseFetch(
            env.SUPABASE_URL,
            env.SUPABASE_SERVICE_ROLE_KEY,
            'static_vessel_cache',
            `imo=eq.${cleanImo}`,
            'DELETE'
          );
        }

        return jsonResponse({
          success: true,
          imo: cleanImo,
          message: 'Vessel removed successfully',
          workflowTriggered: false
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: List Tracked Vessels
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/vessels/list' && request.method === 'GET') {
        const vessels = await supabaseFetch(
          env.SUPABASE_URL,
          env.SUPABASE_SERVICE_ROLE_KEY,
          'tracked_imos',
          'select=imo',
          'GET'
        );

        return jsonResponse({
          success: true,
          count: vessels.length,
          vessels: vessels.map(v => v.imo)
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: CallMeBot proxy — avoids browser CORS block
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/callmebot/test' && request.method === 'POST') {
        const body = await request.json();
        const { phone, apikey, message } = body;

        if (!phone || !apikey) {
          return jsonResponse({ error: 'phone and apikey required' }, 400, corsHeaders);
        }

        const text = encodeURIComponent(message || 'VesselTracker test alert 🚢');
        const url  = `https://api.callmebot.com/whatsapp.php?phone=${encodeURIComponent(phone)}&text=${text}&apikey=${encodeURIComponent(apikey)}`;

        try {
          const r = await fetch(url, { method: 'GET' });
          const responseText = await r.text();
          if (r.ok || r.status === 200) {
            return jsonResponse({ success: true, status: r.status, response: responseText }, 200, corsHeaders);
          } else {
            return jsonResponse({ success: false, status: r.status, response: responseText }, 200, corsHeaders);
          }
        } catch (err) {
          return jsonResponse({ success: false, error: err.message }, 200, corsHeaders);
        }
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: Weather proxy — multi-API fallback chain, all server-side
      // Wind:  Open-Meteo → MET Norway (Yr.no) → 7Timer → wttr.in
      // Wave:  Open-Meteo Marine → Open-Meteo wind-wave model
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/weather' && request.method === 'GET') {
        const params = url.searchParams;
        const lat = parseFloat(params.get('lat'));
        const lon = parseFloat(params.get('lon'));

        if (isNaN(lat) || isNaN(lon)) {
          return jsonResponse({ error: 'lat and lon required' }, 400, corsHeaders);
        }

        const latF = lat.toFixed(4), lonF = lon.toFixed(4);
        let wind = null, wave = null;

        // ── PROVIDER 1: Open-Meteo (wind + wave) ─────────────────────────────
        try {
          const [marineRes, forecastRes] = await Promise.all([
            fetch(`https://marine-api.open-meteo.com/v1/marine?latitude=${latF}&longitude=${lonF}&current=wave_height`),
            fetch(`https://api.open-meteo.com/v1/forecast?latitude=${latF}&longitude=${lonF}&current=wind_speed_10m&wind_speed_unit=kn`),
          ]);
          if (forecastRes.ok) {
            const d = await forecastRes.json();
            if (d.current?.wind_speed_10m != null) wind = Number(d.current.wind_speed_10m);
          }
          if (marineRes.ok) {
            const d = await marineRes.json();
            if (d.current?.wave_height != null) wave = Number(d.current.wave_height);
          }
        } catch (_) {}

        // ── PROVIDER 2: MET Norway / Yr.no (wind fallback) ───────────────────
        // wind_speed in m/s → knots (×1.944)
        if (wind === null) {
          try {
            const r = await fetch(
              `https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=${latF}&lon=${lonF}`,
              { headers: { 'User-Agent': 'VesselTracker/5.5 github.com/asmahri2-afk' } }
            );
            if (r.ok) {
              const d = await r.json();
              const ms = d.properties?.timeseries?.[0]?.data?.instant?.details?.wind_speed;
              if (ms != null) wind = Number(ms) * 1.944;
            }
          } catch (_) {}
        }

        // ── PROVIDER 3: 7Timer (wind fallback) ───────────────────────────────
        // Uses Beaufort scale — map to knot midpoints
        if (wind === null) {
          try {
            const r = await fetch(
              `https://www.7timer.info/bin/api.pl?lon=${lonF}&lat=${latF}&product=meteo&output=json`
            );
            if (r.ok) {
              const d = await r.json();
              const bft = d.dataseries?.[0]?.wind10m?.speed;
              const bftKnots = { 1: 2, 2: 5, 3: 9, 4: 13, 5: 19, 6: 25, 7: 32, 8: 40 };
              if (bft != null) wind = bftKnots[bft] ?? bft * 3;
            }
          } catch (_) {}
        }

        // ── PROVIDER 4: wttr.in (wind fallback) ──────────────────────────────
        // windspeedKmph → knots (÷1.852)
        if (wind === null) {
          try {
            const r = await fetch(`https://wttr.in/${latF},${lonF}?format=j1`);
            if (r.ok) {
              const d = await r.json();
              const kmh = d.current_condition?.[0]?.windspeedKmph;
              if (kmh != null) wind = Number(kmh) / 1.852;
            }
          } catch (_) {}
        }

        // ── PROVIDER 5: Open-Meteo wind-wave model (wave fallback) ───────────
        if (wave === null) {
          try {
            const r = await fetch(
              `https://api.open-meteo.com/v1/forecast?latitude=${latF}&longitude=${lonF}&current=wind_wave_height`
            );
            if (r.ok) {
              const d = await r.json();
              if (d.current?.wind_wave_height != null) wave = Number(d.current.wind_wave_height);
            }
          } catch (_) {}
        }

        return jsonResponse({
          wave: wave !== null ? Math.round(wave * 10) / 10 : null,
          wind: wind !== null ? Math.round(wind * 10) / 10 : null,
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Draft — Load
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/draft' && request.method === 'GET') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const user = await userRes.json();

        const imo = url.searchParams.get('imo');
        if (!imo) return jsonResponse({ error: 'imo required' }, 400, corsHeaders);

        const rows = await supabaseFetch(
          env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'sof_drafts', `imo=eq.${imo}&user_id=eq.${user.id}&select=data,notes,updated_at`, 'GET'
        );

        if (!rows.length) return jsonResponse({ draft: null }, 200, corsHeaders);
        return jsonResponse({ draft: rows[0].data, notes: rows[0].notes, updated_at: rows[0].updated_at }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Draft — Save
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/draft' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const user = await userRes.json();

        const body = await request.json();
        const { imo, data, notes } = body;
        if (!imo || !data) return jsonResponse({ error: 'imo and data required' }, 400, corsHeaders);

        // Upsert — update if exists, insert if not
        const existing = await supabaseFetch(
          env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'sof_drafts', `imo=eq.${String(imo)}&user_id=eq.${user.id}&select=id`, 'GET'
        );

        if (existing.length > 0) {
          await supabaseFetch(
            env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_drafts', `imo=eq.${String(imo)}&user_id=eq.${user.id}`, 'PATCH',
            { data, notes: notes || null, updated_at: new Date().toISOString() }
          );
        } else {
          await supabaseFetch(
            env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_drafts', null, 'POST',
            { imo: String(imo), user_id: user.id, data, notes: notes || null, updated_at: new Date().toISOString() }
          );
        }

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Draft — Delete
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/draft' && request.method === 'DELETE') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const user = await userRes.json();

        const imo = url.searchParams.get('imo');
        if (!imo) return jsonResponse({ error: 'imo required' }, 400, corsHeaders);

        await supabaseFetch(
          env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'sof_drafts', `imo=eq.${imo}&user_id=eq.${user.id}`, 'DELETE'
        );

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: List all users (for handoff recipient picker)
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/users/list' && request.method === 'GET') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const me = await userRes.json();

        const users = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles', `select=id,username&id=neq.${me.id}&order=username.asc`, 'GET');

        console.log(`users/list: me=${me.id}, found ${users.length} other users`);
        return jsonResponse({ users }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Handoff — Send
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/handoff/send' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const me = await userRes.json();

        const body = await request.json();
        const { to_user_id, imo, vessel_name, draft_data, notes } = body;
        if (!to_user_id || !imo || !draft_data) return jsonResponse({ error: 'to_user_id, imo and draft_data required' }, 400, corsHeaders);

        // Get sender username
        const myProfile = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles', `id=eq.${me.id}&select=username`, 'GET');
        const fromUsername = myProfile[0]?.username || 'Unknown';

        // Get recipient username
        const toProfile = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'user_profiles', `id=eq.${to_user_id}&select=username`, 'GET');
        if (!toProfile.length) return jsonResponse({ error: 'Recipient not found' }, 404, corsHeaders);
        const toUsername = toProfile[0].username;

        await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'sof_handoffs', null, 'POST', {
            from_user_id: me.id,
            to_user_id,
            from_username: fromUsername,
            to_username: toUsername,
            imo: String(imo),
            vessel_name: vessel_name || '',
            draft_data,
            notes: notes || null,
            status: 'pending',
            sender_notified: false,
          });

        return jsonResponse({ success: true, to_username: toUsername }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Handoff — Get pending (incoming + unseen declines for sender)
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/handoff/pending' && request.method === 'GET') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const me = await userRes.json();

        const [incoming, declines] = await Promise.all([
          // Pending handoffs sent TO me
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_handoffs',
            `to_user_id=eq.${me.id}&status=eq.pending&select=id,from_username,imo,vessel_name,notes,created_at`,
            'GET'),
          // Handoffs I sent that were declined and I haven't seen yet
          supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_handoffs',
            `from_user_id=eq.${me.id}&status=eq.declined&sender_notified=eq.false&select=id,to_username,imo,vessel_name`,
            'GET'),
        ]);

        return jsonResponse({
          incoming,
          declines,
          total: incoming.length + declines.length,
        }, 200, corsHeaders);
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Handoff — Respond (accept or decline)
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/handoff/respond' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const me = await userRes.json();

        const body = await request.json();
        const { id, action } = body; // action: 'accept' | 'decline'
        if (!id || !['accept', 'decline'].includes(action)) return jsonResponse({ error: 'id and action required' }, 400, corsHeaders);

        // Get the handoff
        const handoffs = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
          'sof_handoffs', `id=eq.${id}&to_user_id=eq.${me.id}&status=eq.pending&select=*`, 'GET');
        if (!handoffs.length) return jsonResponse({ error: 'Handoff not found' }, 404, corsHeaders);
        const handoff = handoffs[0];

        if (action === 'accept') {
          // 1. Save draft to recipient's sof_drafts (upsert)
          const existingDraft = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_drafts', `imo=eq.${handoff.imo}&user_id=eq.${me.id}&select=id`, 'GET');
          if (existingDraft.length > 0) {
            await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
              'sof_drafts', `imo=eq.${handoff.imo}&user_id=eq.${me.id}`, 'PATCH',
              { data: handoff.draft_data, notes: handoff.notes, updated_at: new Date().toISOString() });
          } else {
            await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
              'sof_drafts', null, 'POST',
              { imo: handoff.imo, user_id: me.id, data: handoff.draft_data, notes: handoff.notes, updated_at: new Date().toISOString() });
          }

          // 2. Add vessel to recipient's fleet if not already tracked
          const alreadyTracked = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'tracked_imos', `imo=eq.${handoff.imo}&user_id=eq.${me.id}&select=imo`, 'GET');
          if (!alreadyTracked.length) {
            await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
              'tracked_imos', null, 'POST', { imo: handoff.imo, user_id: me.id });
          }

          // 3. Mark handoff accepted
          await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_handoffs', `id=eq.${id}`, 'PATCH', { status: 'accepted' });

          return jsonResponse({ success: true, action: 'accepted', vessel_added: !alreadyTracked.length }, 200, corsHeaders);

        } else {
          // Decline — mark declined, sender_notified = false so sender gets notified
          await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_handoffs', `id=eq.${id}`, 'PATCH', { status: 'declined', sender_notified: false });

          return jsonResponse({ success: true, action: 'declined' }, 200, corsHeaders);
        }
      }

      // ─────────────────────────────────────────────────────────────────────────
      // ROUTE: SOF Handoff — Acknowledge decline notifications
      // ─────────────────────────────────────────────────────────────────────────
      if (path === '/sof/handoff/ack' && request.method === 'POST') {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader?.startsWith('Bearer ')) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const token = authHeader.slice(7);
        const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
          headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${token}` },
        });
        if (!userRes.ok) return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        const me = await userRes.json();

        const body = await request.json();
        const ids = body.ids || [];
        if (!ids.length) return jsonResponse({ success: true }, 200, corsHeaders);

        // Mark all as sender_notified = true
        for (const id of ids) {
          await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
            'sof_handoffs', `id=eq.${id}&from_user_id=eq.${me.id}`, 'PATCH', { sender_notified: true });
        }

        return jsonResponse({ success: true }, 200, corsHeaders);
      }

      // Default 404
      return jsonResponse({ error: 'Not found' }, 404, corsHeaders);

    } catch (error) {
      console.error('Worker error:', error);
      return jsonResponse({
        error: 'Internal server error',
        message: error.message
      }, 500, corsHeaders);
    }
  }
};

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

async function triggerGitHubWorkflow(token, repo, branch) {
  if (!token || !repo) {
    console.warn('GitHub credentials missing — set GITHUB_TOKEN and GITHUB_REPO in Worker env vars');
    return false;
  }

  try {
    const [owner, repoName] = repo.split('/');

    // ─────────────────────────────────────────────────────────────────────────
    // FIX 2: Correct workflow filename — removed the "__2_" artifact suffix.
    // The real file in your GitHub repo is "update_vessels.yml".
    // If your file has a different name, update this to match exactly.
    // ─────────────────────────────────────────────────────────────────────────
    const workflowFile = 'update_vessels.yml';
    const url = `https://api.github.com/repos/${owner}/${repoName}/actions/workflows/${workflowFile}/dispatches`;

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `token ${token}`,
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json',
        'User-Agent': 'Cloudflare-Worker-Vessel-Manager'
      },
      body: JSON.stringify({ ref: branch })
    });

    if (response.status === 204) {
      console.log('✅ GitHub workflow triggered successfully');
      return true;
    } else {
      const text = await response.text();
      console.error(`❌ GitHub workflow trigger failed: ${response.status} — ${text}`);
      return false;
    }
  } catch (error) {
    console.error('Error triggering workflow:', error);
    return false;
  }
}

async function supabaseFetch(url, key, table, query, method, body = null) {
  const endpoint = query
    ? `${url}/rest/v1/${table}?${query}`
    : `${url}/rest/v1/${table}`;

  const options = {
    method,
    headers: {
      'apikey': key,
      'Authorization': `Bearer ${key}`,
      'Content-Type': 'application/json',
      'Prefer': 'return=minimal'
    }
  };

  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(endpoint, options);

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Supabase error (${response.status}): ${text}`);
  }

  if (method === 'DELETE' || method === 'PATCH' || response.status === 204 || response.status === 201) {
    return [];
  }

  // Guard against any other unexpected empty body
  const text = await response.text();
  if (!text) return [];
  return JSON.parse(text);
}

// ── Admin auth helper ─────────────────────────────────────────────────────────
async function verifyAdmin(env, token) {
  const userRes = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, {
    headers: {
      'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
      'Authorization': `Bearer ${token}`,
    },
  });
  if (!userRes.ok) return { ok: false, error: 'Unauthorized', status: 401 };
  const user = await userRes.json();
  const profile = await supabaseFetch(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY,
    'user_profiles', `id=eq.${user.id}&select=username`, 'GET');
  if (!profile.length || profile[0].username !== 'asmahri') {
    return { ok: false, error: 'Forbidden', status: 403 };
  }
  return { ok: true };
}

function jsonResponse(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...headers
    }
  });
}
