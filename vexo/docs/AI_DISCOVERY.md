# AI-Assisted Discovery Feature

## Overview

This feature adds AI-powered music discovery using an Ollama endpoint. It provides:

1. **`/play ai` command** - Queue a seed song with AI-suggested follow-ups
2. **Join-triggered recommendations** - Queue personalized suggestions when users join voice channels
3. **AI-preferred autoplay** - Use AI for discovery when enabled and Ollama is available

## Configuration

### Environment Variables

Add to your `.env` file:

```env
# Ollama AI Service
OLLAMA_BASE_URL=https://ollama.plingindigo.org
OLLAMA_MODEL=llama3
```

### Guild Settings

Two new per-guild settings are available via the dashboard or API:

- `ai_discovery_enabled` (default: false) - Enable AI as the preferred discovery method for autoplay
- `ai_discovery_on_join` (default: false) - Queue personalized AI suggestions when users join VC

Access via dashboard at `/settings` or API at `/api/guilds/{guild_id}/settings`

## Features

### 1. `/play ai <seed_query>`

**Usage:**
```
/play ai Bohemian Rhapsody
/play ai The Beatles - Hey Jude
```

**Behavior:**
1. Resolves the seed query to a playable track (same as `/play song`)
2. Queues the seed track immediately
3. Requests AI suggestions based on the seed (title, artist, genre, year)
4. Resolves and queues the best 3-5 AI suggestions
5. If Ollama is unavailable, returns ephemeral error: "AI service is currently unavailable"

**No silent fallback** - the command explicitly requires AI and will fail gracefully if unavailable.

### 2. Join-Triggered Personalized Recommendations

**Requirements:**
- `ai_discovery_on_join` setting enabled for the guild
- Ollama service available
- User joins a voice channel where the bot is active

**Behavior:**
1. Fetches joining user's liked and disliked tracks
2. Fetches disliked tracks from all other VC members (VC-wide veto)
3. Sends preferences to Ollama for personalized suggestions
4. Resolves suggestions to playable tracks
5. Queues up to 5 tracks (applies hard filters: dislikes + recent history)

**Hard Filters:**
- Never queue tracks disliked by the joining user
- Never queue tracks disliked by any other VC member
- Never queue recently played tracks (last 100 per guild)

**Debounce:** 30 seconds per user per guild to avoid spam on quick channel switches

**Silent failures:** If AI is unavailable at join time, no errors are shown in chat

### 3. AI-Preferred Discovery for Autoplay

When `ai_discovery_enabled` is true and Ollama is healthy:
- Autoplay/discovery will use AI suggestions as the preferred method
- Falls back to traditional discovery if AI returns no results or is unavailable
- Uses collective preferences from VC members (democratic user selection)
- Considers current/last track as seed if available

**Fallback chain:** AI discovery → Traditional discovery → Chart fallback

## Implementation Details

### Ollama Client (`src/services/ollama_client.py`)

- **Health check caching:** 45-second TTL to reduce API calls
- **Strict JSON output:** Prompts request JSON-only responses
- **Graceful failures:** Invalid JSON or timeouts treated as service unavailable
- **Two suggestion modes:**
  - `suggest_from_seed()` - Based on single seed track
  - `suggest_for_user()` - Based on user likes/dislikes and group vetoes

### Database Extensions

**ReactionCRUD new methods:**
- `get_disliked_songs(user_id, limit)` - Get user's disliked tracks
- `get_disliked_songs_for_users(user_ids, limit_per_user)` - Batch fetch dislikes (efficient, uses window functions)

### Guild Settings

Stored in `guild_settings` table with keys:
- `ai_discovery_enabled`
- `ai_discovery_on_join`

Retrieved via `GuildCRUD.get_setting()` and set via `GuildCRUD.set_setting()`

### Voice State Handler

Enhanced `on_voice_state_update` in `MusicCog`:
- Detects user joining bot's voice channel
- Debounces per-user with 30-second window
- Calls `_handle_user_join_for_ai_discovery()` asynchronously

## Testing

### Manual Testing

1. **Test `/play ai` command:**
   ```
   /play ai Daft Punk - Get Lucky
   ```
   Should queue seed + 3-5 AI suggestions

2. **Test AI unavailable fallback:**
   - Stop Ollama service temporarily
   - Run `/play ai <song>` - should return ephemeral error

3. **Test join-triggered discovery:**
   - Enable `ai_discovery_on_join` for a guild
   - Have a user with liked/disliked tracks join VC
   - Check queue for personalized suggestions

4. **Test AI autoplay:**
   - Enable `ai_discovery_enabled` for a guild
   - Let queue empty with autoplay on
   - Next track should use AI discovery

### Health Check Logging

Check logs for:
- `Ollama health check passed/failed` (DEBUG/WARNING level)
- `AI discovery returned no results` (DEBUG level)
- `AI join discovery queued X tracks` (INFO level)

## Troubleshooting

### "AI service is currently unavailable"

- Check `OLLAMA_BASE_URL` is reachable
- Verify Ollama is running and model is loaded
- Check health endpoint: `curl https://ollama.plingindigo.org/api/tags`

### AI suggestions not queuing

- Check guild settings are enabled
- Verify user has liked tracks in database
- Check logs for JSON parsing errors
- Ensure max_song_duration setting isn't filtering all suggestions

### Join-triggered discovery not working

- Verify `ai_discovery_on_join` is enabled
- Check user has liked tracks (minimum 1)
- Ensure bot is in voice channel when user joins
- Review debounce window (30s cooldown)

## Prompts

The AI prompts are designed for strict JSON output:

**System Prompt:**
```
You are a music recommendation AI. You respond ONLY with valid JSON. No markdown, no explanations, just pure JSON.
```

**Expected JSON Schema:**
```json
{
  "suggestions": [
    {"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"},
    ...
  ]
}
```

## Performance Considerations

- **Health check caching:** Reduces Ollama API calls by ~95%
- **Batch dislike queries:** Single query for all VC members
- **Async AI calls:** Non-blocking with strict timeouts (25s default)
- **Discovery timeout:** 20s max for entire discovery operation
- **Resolution parallelization:** Could be added in future for faster queuing

## Future Enhancements

- [ ] Parallel track resolution for faster AI suggestion queuing
- [ ] Per-guild Ollama model selection
- [ ] AI suggestion confidence scores
- [ ] User feedback loop (track AI suggestion quality)
- [ ] Smart re-prompting when all suggestions filtered
- [ ] Context-aware prompts (time of day, mood detection)
