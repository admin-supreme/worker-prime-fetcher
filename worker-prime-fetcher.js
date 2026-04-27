import { createClient } from "@libsql/client/web";

const CPU_GUARD_MS = 45_000;
const MAX_PER_RUN = 13;

function cleanText(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed === "" ? null : trimmed;
  }
  const str = String(value).trim();
  return str === "" ? null : str;
}

function parseJsonArrayIfPossible(value) {
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (!trimmed || trimmed === "[]") return null;

  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) return parsed;
  } catch (_) {
    // Not JSON, fall through.
  }

  return value;
}

function toPlainListText(value, options = {}) {
  const {
    separator = ", ",
    mapItem = null,
  } = options;

  if (value === null || value === undefined) return null;

  let list = value;

  if (typeof list === "string") {
    const parsed = parseJsonArrayIfPossible(list);
    if (parsed === null) return null;
    if (!Array.isArray(parsed)) {
      const text = cleanText(parsed);
      return text;
    }
    list = parsed;
  }

  if (!Array.isArray(list)) {
    return cleanText(list);
  }

  const items = list
    .map((item) => {
      if (item === null || item === undefined) return null;

      if (mapItem) {
        return cleanText(mapItem(item));
      }

      if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") {
        return cleanText(item);
      }

      if (typeof item === "object") {
        if (typeof item.name === "string") return cleanText(item.name);
        if (typeof item.title === "string") return cleanText(item.title);
        if (typeof item.url === "string") return cleanText(item.url);
        return cleanText(JSON.stringify(item));
      }

      return cleanText(String(item));
    })
    .filter(Boolean);

  return items.length ? items.join(separator) : null;
}

function joinNameList(value) {
  return toPlainListText(value, {
    separator: ", ",
    mapItem: (item) => {
      if (item && typeof item === "object" && typeof item.name === "string") {
        return item.name;
      }
      return item;
    },
  });
}

function getTargetEndpoint(env) {
  const endpoint = cleanText(env?.TARGET_ENDPOINT);
  if (!endpoint) {
    throw new Error("Missing TARGET_ENDPOINT environment variable.");
  }
  return endpoint;
}

function shouldStopForCpu(event) {
  const scheduledTime = Number(event?.scheduledTime ?? 0);
  return scheduledTime > 0 && Date.now() - scheduledTime > CPU_GUARD_MS;
}

async function triggerValTownPipeline(env) {
  const endpoint = getTargetEndpoint(env);

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

async function fetchJikan(page) {
  try {
    const res = await fetch(`https://api.jikan.moe/v4/anime?page=${page}`);

    if (!res.ok) {
      await res.text().catch(() => "");
      return { data: [], pagination: null };
    }

    return await res.json();
  } catch (_) {
    return { data: [], pagination: null };
  }
}

async function fetchHighResPoster(env, title, year) {
  try {
    const apiKey = cleanText(env?.TMDB_API_KEY);
    if (!apiKey) return null;

    const query = encodeURIComponent(year ? `${title} ${year}` : title);
    const tmdbUrl = `https://api.themoviedb.org/3/search/multi?api_key=${apiKey}&query=${query}`;

    const tmdbRes = await fetch(tmdbUrl, { cf: { cacheTtl: 300 } });

    if (!tmdbRes.ok) {
      return null;
    }

    const tmdbData = await tmdbRes.json().catch(() => null);
    const results = tmdbData?.results;

    if (!Array.isArray(results)) return null;

    const animeMatch = results.find((item) => {
      const hasPoster = Boolean(item?.poster_path);
      const isAnime = item?.genre_ids?.includes(16);
      const yearMatches = !year || item?.first_air_date?.startsWith(String(year));
      return hasPoster && isAnime && yearMatches;
    });

    if (animeMatch?.poster_path) {
      return `https://image.tmdb.org/t/p/original${animeMatch.poster_path}`;
    }

    return null;
  } catch (err) {
    console.log("TMDB FETCH FAILED:", err);
    return null;
  }
}

function mapJikanStatus(status) {
  if (status === "Currently Airing") return "AIRING";
  if (status === "Finished Airing") return "COMPLETED";
  if (status === "Not yet aired") return "UPCOMING";
  return null;
}

function generateSlug(title) {
  return String(title || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-");
}

function transform(media) {
  const title = cleanText(media?.title_english || media?.title) || "untitled";
  const slug = `${generateSlug(title)}-${media?.mal_id}`;

  return {
    id: slug,
    mal_id: media?.mal_id ?? null,
    title,
    title_japanese: cleanText(media?.title_japanese),
    title_synonyms: toPlainListText(media?.title_synonyms),
    year: media?.year ?? null,
    season: cleanText(media?.season),
    type: cleanText(media?.type),
    studio: cleanText(media?.studios?.[0]?.name),
    studios: joinNameList(media?.studios),
    audio: "SUB",
    dubbed_languages: null,
    duration: cleanText(media?.duration),
    episodes: media?.episodes ?? 0,
    tags: joinNameList(media?.genres),
    age_rating: cleanText(media?.rating),
    total_seasons: 1,
    airing_date: media?.aired?.from ? media.aired.from.split("T")[0] : null,
    ended_date: media?.aired?.to ? media.aired.to.split("T")[0] : null,
    airing_status: mapJikanStatus(media?.status),
    image_url: null,
    overview: cleanText(media?.synopsis),
    producers: joinNameList(media?.producers),
    licensors: joinNameList(media?.licensors),
    themes: joinNameList(media?.themes),
    demographics: joinNameList(media?.demographics),
    trailer: cleanText(media?.trailer?.youtube_id),
    source: cleanText(media?.source),
    popularity: media?.popularity ?? null,
    rating: media?.score ?? null,
    scored_by: media?.scored_by ?? null,
    favorites: media?.favorites ?? null,
    aired_from_full: cleanText(media?.aired?.from),
    aired_to_full: cleanText(media?.aired?.to),
    broadcast: cleanText(media?.broadcast?.string),
    background: cleanText(media?.background),
  };
}

async function upsertAnime(db, anime) {
  const columns = [
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
    "scored_by",
    "favorites",
    "aired_from_full",
    "aired_to_full",
    "broadcast",
    "background",
  ];

  const values = columns.map((key) => (anime[key] === undefined ? null : anime[key]));
  const placeholders = columns.map(() => "?").join(", ");

  await db.execute({
    sql: `
      INSERT INTO anime_info (
        ${columns.join(", ")}
      )
      VALUES (
        ${placeholders}
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
        audio = excluded.audio,
        dubbed_languages = excluded.dubbed_languages,
        duration = excluded.duration,
        episodes = excluded.episodes,
        tags = excluded.tags,
        age_rating = excluded.age_rating,
        total_seasons = excluded.total_seasons,
        airing_date = excluded.airing_date,
        ended_date = excluded.ended_date,
        airing_status = excluded.airing_status,
        image_url = CASE
          WHEN anime_info.image_url LIKE '%image.tmdb.org%' THEN anime_info.image_url
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
        scored_by = excluded.scored_by,
        favorites = excluded.favorites,
        aired_from_full = excluded.aired_from_full,
        aired_to_full = excluded.aired_to_full,
        broadcast = excluded.broadcast,
        background = excluded.background,
        updated_at = CURRENT_TIMESTAMP
    `,
    args: values,
  });
}

async function ownPipelineSafe(env) {
  if (typeof ownPipeline === "function") {
    await ownPipeline(env);
  }
}

async function syncJikan(env, db, event) {
  const state = env.STATE ?? null;
  const overrideApplied = state ? await state.get("jikan_override_applied") : null;

  let page;
  if (cleanText(env.START_PAGE) && !overrideApplied) {
    page = parseInt(env.START_PAGE, 10);
    if (isNaN(page) || page < 1) page = 1;

    if (state) {
      await state.put("jikan_page", String(page));
      await state.put("jikan_offset", "0");
      await state.put("jikan_override_applied", "true");
    }

    console.log("Manual START_PAGE applied ONCE:", page);
  } else {
    page = parseInt((state ? (await state.get("jikan_page")) || "1" : "1"), 10);
    if (isNaN(page) || page < 1) page = 1;
  }

  let offset = parseInt((state ? (await state.get("jikan_offset")) || "0" : "0"), 10);
  if (isNaN(offset) || offset < 0) offset = 0;

  console.log("Fetching Jikan page:", page);

  const result = await fetchJikan(page);
  const mediaList = Array.isArray(result?.data) ? result.data : [];
  const hasNext = Boolean(result?.pagination?.has_next_page);

  if (mediaList.length === 0) {
    if (state) {
      await state.put("jikan_page", "1");
      await state.put("jikan_offset", "0");
    }
    return;
  }

  const batch = mediaList.slice(offset, offset + MAX_PER_RUN);
  let processed = 0;

  for (const media of batch) {
    if (processed >= MAX_PER_RUN) break;
    if (shouldStopForCpu(event)) {
      console.log("Stopping early to prevent CPU exceed");
      break;
    }

    processed++;

    try {
      const transformed = transform(media);

      const tmdbPoster = await fetchHighResPoster(
        env,
        transformed.title,
        transformed.year
      );

      transformed.image_url =
        tmdbPoster || media?.images?.jpg?.large_image_url || null;

      await upsertAnime(db, transformed);
    } catch (err) {
      console.error("UPSERT FAILED:", err);
    }
  }

  const newOffset = offset + MAX_PER_RUN;

  if (state) {
    if (newOffset >= mediaList.length) {
      const nextPage = hasNext ? page + 1 : 1;
      await state.put("jikan_page", String(nextPage));
      await state.put("jikan_offset", "0");
    } else {
      await state.put("jikan_offset", String(newOffset));
    }
  }
}

async function refreshMissingImages(env, db, event) {
  const state = env.STATE ?? null;
  const overrideApplied = state ? await state.get("refresh_override_applied") : null;

  let lastId;
  if (cleanText(env.REFRESH_START_ID) && !overrideApplied) {
    lastId = env.REFRESH_START_ID;

    if (state) {
      await state.put("refresh_last_id", lastId);
      await state.put("refresh_override_applied", "true");
    }

    console.log("Manual REFRESH_START_ID applied ONCE:", lastId);
  } else {
    lastId = state ? (await state.get("refresh_last_id")) || "" : "";
  }

  console.log("Refresh starting after ID:", lastId);

  const result = await db.execute({
    sql: `
      SELECT id, title, year, image_url
      FROM anime_info
      WHERE (image_url IS NULL OR image_url NOT LIKE '%image.tmdb.org%')
        AND id > ?
      ORDER BY id
      LIMIT ?
    `,
    args: [lastId, MAX_PER_RUN],
  });

  const rows = Array.isArray(result?.rows) ? result.rows : [];

  if (rows.length === 0) {
    console.log("Refresh completed. Resetting cursor.");
    if (state) {
      await state.put("refresh_last_id", "");
      await state.delete("refresh_override_applied");
    }
    return;
  }

  let newLastId = lastId;
  let processed = 0;

  for (const anime of rows) {
    if (processed >= MAX_PER_RUN) break;
    if (shouldStopForCpu(event)) {
      console.log("Stopping refresh early to prevent CPU exceed");
      break;
    }

    processed++;

    try {
      const tmdbPoster = await fetchHighResPoster(
        env,
        anime.title,
        anime.year
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
      }

      newLastId = anime.id;
    } catch (err) {
      console.log("REFRESH FAILED:", anime.title, err);
    }
  }

  if (state) {
    await state.put("refresh_last_id", newLastId);
  }
}

export default {
  async fetch(request, env, ctx) {
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
      } catch (error) {
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

  async scheduled(event, env, ctx) {
    if (!env.LIBSQL_DB_URL || !env.LIBSQL_DB_AUTH_TOKEN) {
      console.error("Missing Turso configuration.");
      return;
    }

    const db = createClient({
      url: env.LIBSQL_DB_URL.trim(),
      authToken: env.LIBSQL_DB_AUTH_TOKEN.trim(),
    });

    ctx.waitUntil(
      (async () => {
        try {
          await ownPipelineSafe(env);

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
