import { sqliteTable, text, integer, real } from 'drizzle-orm/sqlite-core';

// Schema matching the Python bot's database

export const users = sqliteTable('users', {
    id: integer('id').primaryKey(),
    username: text('username').notNull(),
    created_at: text('created_at'),
    last_active: text('last_active'),
    is_banned: integer('is_banned', { mode: 'boolean' }).default(false),
    opted_out: integer('opted_out', { mode: 'boolean' }).default(false),
});

export const songs = sqliteTable('songs', {
    id: integer('id').primaryKey(),
    canonical_yt_id: text('canonical_yt_id').notNull(),
    title: text('title').notNull(),
    artist_name: text('artist_name').notNull(),
    album: text('album'),
    release_year: integer('release_year'),
    duration_seconds: integer('duration_seconds'),
    spotify_id: text('spotify_id'),
    is_ephemeral: integer('is_ephemeral', { mode: 'boolean' }).default(false),
    created_at: text('created_at'),
});

export const guilds = sqliteTable('guilds', {
    id: integer('id').primaryKey(),
    name: text('name'),
    created_at: text('created_at'),
});

export const playbackSessions = sqliteTable('playback_sessions', {
    id: text('id').primaryKey(),
    guild_id: integer('guild_id'),
    channel_id: integer('channel_id'),
    started_at: text('started_at'),
    ended_at: text('ended_at'),
});

export const playbackHistory = sqliteTable('playback_history', {
    id: integer('id').primaryKey(),
    session_id: text('session_id'),
    song_id: integer('song_id'),
    played_at: text('played_at'),
    completed: integer('completed', { mode: 'boolean' }).default(false),
    skip_reason: text('skip_reason'),
    discovery_source: text('discovery_source'),
    discovery_reason: text('discovery_reason'),
    for_user_id: integer('for_user_id'),
});

export const songReactions = sqliteTable('song_reactions', {
    id: integer('id').primaryKey(),
    song_id: integer('song_id'),
    user_id: integer('user_id'),
    reaction: text('reaction'),
    created_at: text('created_at'),
});

export const songGenres = sqliteTable('song_genres', {
    id: integer('id').primaryKey(),
    song_id: integer('song_id'),
    genre: text('genre'),
});

export const songLibraryEntries = sqliteTable('song_library_entries', {
    id: integer('id').primaryKey(),
    song_id: integer('song_id'),
    user_id: integer('user_id'),
    source: text('source'),
    added_at: text('added_at'),
});

export const userPreferences = sqliteTable('user_preferences', {
    id: integer('id').primaryKey(),
    user_id: integer('user_id'),
    preference_type: text('preference_type'),
    preference_key: text('preference_key'),
    affinity_score: real('affinity_score'),
});

export const importedPlaylists = sqliteTable('imported_playlists', {
    id: integer('id').primaryKey(),
    user_id: integer('user_id'),
    platform: text('platform'),
    name: text('name'),
    track_count: integer('track_count'),
    imported_at: text('imported_at'),
});
