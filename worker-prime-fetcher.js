import { createClient } from "@libsql/client/web";

const ANIME_COLUMNS = [
  "id",
  "type",
  "title",
  "title_japanese",
  "title_synonyms",
  "mal_id",
  "year",
  "season",
  "studio",
  "studios",
  "audio",
  "dubbed_languages",
  "duration",
  "episodes",
  "tags",
  "age_rating",
  "total_seasons",
  "airing_date",
  "ended_date",
  "airing_status",
  "image_url",
  "overview",
  "producers",
  "licensors",
  "themes",
  "demographics",
  "trailer",
  "source",
  "popularity",
  "rating",
  "rank",
  "top_genre_rank",
  "scored_by",
  "members",
  "favorites",
  "aired_from_full",
  "aired_to_full",
  "broadcast",
  "background",
  "openings",
  "endings",
  "streaming",
  "external_links",
] as const;

function cleanText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text.length ? text : null;
}

function normalizeList(
  value: unknown,
  options?: { includeUrl?: boolean }
): string | null {
  if (value === null || value === undefined) return null;

  const items = Array.isArray(value) ? value : [value];
  const out: string[] = [];

  for (const item of items) {
    if (item === null || item === undefined) continue;

    if (
      typeof item === "string" ||
      typeof item === "number" ||
      typeof item === "boolean"
    ) {
      const text = cleanText(item);
      if (text) out.push(text);
      continue;
    }

    if (typeof item === "object") {
      const obj = item as Record<string, unknown>;
      const label = cleanText(
        obj.name ??
          obj.title ??
          obj.text ??
          obj.label ??
          obj.site ??
          obj.source ??
          obj.publisher ??
          ""
      );
      const url = cleanText(obj.url ?? obj.link ?? obj.href ?? "");

      if (options?.includeUrl) {
        if (label && url) out.push(`${label} (${url})`);
        else if (label) out.push(label);
        else if (url) out.push(url);
      } else {
        if (label) out.push(label);
        else if (url) out.push(url);
      }
    }
  }

  return out.length ? out.join(", ") : null;
}

function normalizeScalar(value: unknown): string | number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const text = value.trim();
    return text.length ? text : null;
  }
  return value as string | number;
}

function generateSlug(title: string): string {
  return title
    .toLowerCase()
    .replace(/[^\w\s]/g, "")
    .replace(/\s+/g, "-");
}

function mapJikanStatus(status: string | null | undefined): string | null {
  if (status === "Currently Airing") return "AIRING";
  if (status === "Finished Airing") return "COMPLETED";
  if (status === "Not yet aired") return "UPCOMING";
  return null;
}

async function triggerValTownPipeline(env: any) {
  const endpoint = cleanText(env.TARGET_ENDPOINT);
  if (!endpoint) {
    throw new Error("Missing TARGET_ENDPOINT environment variable.");
  }

  const res = await fetch(endpoint, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
    redirect: "follow",
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Val Town trigger failed: ${res.status} ${text}`);
  }

  return await res.text();
}

async function fetchJikan(page: number) {
  try {
    const res = await fetch(`https://api.jikan.moe/v4/anime?page=${page}`);

    if (!res.ok) {
      await res.text().catch(() => "");
      return { data: [], pagination: null };
    }

    return await res.json();
  } catch {
    return { data: [], pagination: null };
  }
}

async function fetchHighResPoster(env: any, title: string, year: number | null) {
  try {
    if (!env.TMDB_API_KEY) return null;

    const query = encodeURIComponent(year ? `${title} ${year}` : title);
    const tvUrl = `https://api.themoviedb.org/3/search/multi?api_key=${env.TMDB_API_KEY}&query=${query}`;

    const tvRes = await fetch(tvUrl, { cf: { cacheTtl: 300 } });

    if (tvRes.ok) {
      const tvData = await tvRes.json().catch(() => null);
      if (tvData?.results) {
        const animeTv = tvData.results.find(
          (r: any) =>
            r.poster_path &&
            r.genre_ids?.includes(16) &&
            (!year || r.first_air_date?.startsWith(String(year)))
        );

        if (animeTv) {
          return `https://image.tmdb.org/t/p/original${animeTv.poster_path}`;
        }
      }
    }

    return null;
  } catch (err) {
    console.log("TMDB FETCH FAILED:", err);
    return null;
  }
}

function transform(media: any) {
  const title = cleanText(media.title_english) || cleanText(media.title) || "";
  const slug = `${generateSlug(title)}-${media.mal_id}`;

  return {
    id: slug,
    mal_id: media.mal_id,
    title,
    year: normalizeScalar(media.year),
    type: normalizeScalar(media.type),
    overview: cleanText(media.synopsis),
    studio: normalizeList(media.studios),
    episodes: normalizeScalar(media.episodes),
    duration: cleanText(media.duration),
    audio: cleanText(media.audio) || "SUB",
    dubbed_languages: normalizeList(
      media.dubbed_languages ?? media.dubbedLanguages ?? null
    ),
    rating: normalizeScalar(media.score),
    popularity: normalizeScalar(media.popularity),
    top_genre_rank: media.rank ? `Top #${media.rank}` : null,
    airing_status: mapJikanStatus(media.status),
    airing_date: media.aired?.from ? String(media.aired.from).split("T")[0] : null,
    tags: normalizeList(media.genres),
    total_seasons: 1,
    title_japanese: cleanText(media.title_japanese),
    title_synonyms: normalizeList(media.title_synonyms),
    source: cleanText(media.source),
    age_rating: cleanText(media.rating),
    scored_by: normalizeScalar(media.scored_by),
    rank: normalizeScalar(media.rank),
    members: normalizeScalar(media.members),
    favorites: normalizeScalar(media.favorites),
    season: cleanText(media.season),
    airing: media.airing ? 1 : 0,
    ended_date: media.aired?.to ? String(media.aired.to).split("T")[0] : null,
    aired_from_full: cleanText(media.aired?.from),
    aired_to_full: cleanText(media.aired?.to),
    broadcast: cleanText(media.broadcast?.string),
    background: cleanText(media.background),
    openings: normalizeList(media.theme?.openings),
    endings: normalizeList(media.theme?.endings),
    streaming: normalizeList(media.streaming, { includeUrl: true }),
    external_links: normalizeList(media.external, { includeUrl: true }),
    studios: normalizeList(media.studios),
    producers: normalizeList(media.producers),
    licensors: normalizeList(media.licensors),
    themes: normalizeList(media.themes),
    demographics: normalizeList(media.demographics),
    trailer: cleanText(media.trailer?.youtube_id),
    image_url: null,
  };
}

async function upsertAnime(db: any, anime: Record<string, unknown>) {
  const values = ANIME_COLUMNS.map((column) => {
    const value = anime[column];
    return value === undefined || value === "" ? null : value;
  });

  await db.execute({
    sql: `
      INSERT INTO anime_info (
        ${ANIME_COLUMNS.join(", ")}
      )
      VALUES (
        ${ANIME_COLUMNS.map(() => "?").join(", ")}
      )
      ON CONFLICT(mal_id) DO UPDATE SET
        id = excluded.id,
        type = excluded.type,
        title = excluded.title,
        title_japanese = excluded.title_japanese,
        title_synonyms = excluded.title_synonyms,
        year = excluded.year,
        season = excluded.season,
        studio = excluded.studio,
        studios = excluded.studios,
        duration = excluded.duration,
        episodes = excluded.episodes,
        tags = excluded.tags,
        age_rating = excluded.age_rating,
        airing_date = excluded.airing_date,
        ended_date = excluded.ended_date,
        airing_status = excluded.airing_status,
        image_url = CASE
          WHEN COALESCE(anime_info.image_url, '') LIKE '%image.tmdb.org%'
          THEN anime_info.image_url
          ELSE excluded.image_url
        END,
        overview = excluded.overview,
        producers = excluded.producers,
        licensors = excluded.licensors,
        themes = excluded.themes,
        demographics = excluded.demographics,
        trailer = excluded.trailer,
        source = excluded.source,
        popularity = excluded.popularity,
        rating = excluded.rating,
        rank = excluded.rank,
        top_genre_rank = excluded.top_genre_rank,
        scored_by = excluded.scored_by,
        members = excluded.members,
        favorites = excluded.favorites,
        aired_from_full = excluded.aired_from_full,
        aired_to_full = excluded.aired_to_full,
        broadcast = excluded.broadcast,
        background = excluded.background,
        openings = excluded.openings,
        endings = excluded.endings,
        streaming = excluded.streaming,
        external_links = excluded.external_links,
        updated_at = CURRENT_TIMESTAMP
    `,
    args: values,
  });
}

async function syncJikan(env: any, db: any, event: any) {
  let page: number;

  const overrideApplied = env.STATE
    ? await env.STATE.get("jikan_override_applied")
    : null;

  if (env.START_PAGE && env.START_PAGE !== "" && !overrideApplied) {
    page = parseInt(env.START_PAGE, 10);
    if (Number.isNaN(page) || page < 1) page = 1;

    if (env.STATE) {
      await env.STATE.put("jikan_page", String(page));
      await env.STATE.put("jikan_offset", "0");
      await env.STATE.put("jikan_override_applied", "true");
    }

    console.log("Manual START_PAGE applied ONCE:", page);
  } else {
    page = parseInt(
      env.STATE ? (await env.STATE.get("jikan_page")) || "1" : "1",
      10
    );
    if (Number.isNaN(page) || page < 1) page = 1;
  }

  let offset = parseInt(
    env.STATE ? (await env.STATE.get("jikan_offset")) || "0" : "0",
    10
  );
  if (Number.isNaN(offset) || offset < 0) offset = 0;

  const MAX_PER_RUN = 13;
  const BATCH_SIZE = MAX_PER_RUN;

  console.log("Fetching Jikan page:", page);

  const result = await fetchJikan(page);
  const mediaList = result.data || [];
  const hasNext = result.pagination?.has_next_page;

  if (!mediaList.length) {
    if (env.STATE) {
      await env.STATE.put("jikan_page", "1");
      await env.STATE.put("jikan_offset", "0");
    }
    return;
  }

  const batch = mediaList.slice(offset, offset + BATCH_SIZE);
  let processed = 0;

  for (const media of batch) {
    if (processed >= MAX_PER_RUN) break;

    if (event?.scheduledTime && Date.now() - event.scheduledTime > 45000) {
      console.log("Stopping early to prevent CPU exceed");
      break;
    }

    processed++;

    try {
      const transformed = transform(media);

      const tmdbPoster = await fetchHighResPoster(
        env,
        transformed.title as string,
        transformed.year as number | null
      );

      transformed.image_url =
        tmdbPoster || media.images?.jpg?.large_image_url || null;

      await upsertAnime(db, transformed);
    } catch (err) {
      console.error("UPSERT FAILED:", err);
    }
  }

  const newOffset = offset + BATCH_SIZE;

  if (env.STATE) {
    if (newOffset >= mediaList.length) {
      const nextPage = hasNext ? page + 1 : 1;
      await env.STATE.put("jikan_page", String(nextPage));
      await env.STATE.put("jikan_offset", "0");
    } else {
      await env.STATE.put("jikan_offset", String(newOffset));
    }
  }
}

async function refreshMissingImages(env: any, db: any, event: any) {
  const MAX_PER_RUN = 13;
  const BATCH_SIZE = MAX_PER_RUN;

  const overrideApplied = env.STATE
    ? await env.STATE.get("refresh_override_applied")
    : null;

  let lastId: string;

  if (env.REFRESH_START_ID && !overrideApplied) {
    lastId = String(env.REFRESH_START_ID);

    if (env.STATE) {
      await env.STATE.put("refresh_last_id", lastId);
      await env.STATE.put("refresh_override_applied", "true");
    }

    console.log("Manual REFRESH_START_ID applied ONCE:", lastId);
  } else {
    lastId = env.STATE ? (await env.STATE.get("refresh_last_id")) || "" : "";
  }

  console.log("Refresh starting after ID:", lastId);

  const result = await db.execute({
    sql: `
      SELECT id, title, year, image_url
      FROM anime_info
      WHERE (image_url IS NULL OR image_url NOT LIKE '%image.tmdb.org%')
        AND COALESCE(image_retry_count, 0) < 3
        AND id > ?
      ORDER BY id
      LIMIT ?
    `,
    args: [lastId, BATCH_SIZE],
  });

  const rows = result.rows || [];

  if (!rows.length) {
    console.log("Refresh completed. Resetting cursor.");
    if (env.STATE) {
      await env.STATE.put("refresh_last_id", "");
      await env.STATE.delete("refresh_override_applied");
    }
    return;
  }

  let newLastId = lastId;
  let processed = 0;

  for (const anime of rows) {
    if (processed >= MAX_PER_RUN) break;

    if (event?.scheduledTime && Date.now() - event.scheduledTime > 45000) {
      console.log("Stopping refresh early to prevent CPU exceed");
      break;
    }

    processed++;

    try {
      const tmdbPoster = await fetchHighResPoster(
        env,
        String(anime.title || ""),
        anime.year ? Number(anime.year) : null
      );

      if (tmdbPoster) {
        await db.execute({
          sql: `
            UPDATE anime_info
            SET image_url = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          args: [tmdbPoster, anime.id],
        });
      } else {
        await db.execute({
          sql: `
            UPDATE anime_info
            SET image_retry_count = COALESCE(image_retry_count, 0) + 1
            WHERE id = ?
          `,
          args: [anime.id],
        });
      }

      newLastId = String(anime.id);
    } catch (err) {
      console.log("REFRESH FAILED:", anime.title);
    }
  }

  if (env.STATE) {
    await env.STATE.put("refresh_last_id", newLastId);
  }
}

export default {
  async fetch(request: Request, env: any) {
    if (request.method === "GET") {
      try {
        const result = await triggerValTownPipeline(env);
        return new Response(
          JSON.stringify({
            message: "Pipeline Triggered Successfully",
            valTownResponse: result,
          }),
          {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }
        );
      } catch (error: any) {
        return new Response(
          JSON.stringify({
            error: "Failed to trigger pipeline",
            details: error?.message || String(error),
          }),
          {
            status: 500,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
    }

    return new Response("Worker is running (cron only).", {
      status: 200,
    });
  },

  async scheduled(event: any, env: any, ctx: any) {
    if (!env.LIBSQL_DB_URL || !env.LIBSQL_DB_AUTH_TOKEN) {
      console.error("Missing Turso configuration.");
      return;
    }

    const db = createClient({
      url: String(env.LIBSQL_DB_URL).trim(),
      authToken: String(env.LIBSQL_DB_AUTH_TOKEN).trim(),
    });

    ctx.waitUntil(
      (async () => {
        try {
          await ownPipeline(env);

          const hourUTC = new Date().getUTCHours();
          if (hourUTC < 20) {
            await syncJikan(env, db, event);
          } else {
            await refreshMissingImages(env, db, event);
          }
        } catch (err) {
          console.error("CRON FATAL ERROR:", err);
        }
      })()
    );
  },
};