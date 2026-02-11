const { Database } = require('bun:sqlite');

// ========================================
// CONFIG
// ========================================
const SPOTIFY_CLIENT_ID = '9419bbfdd85544f1a3e0db23f91f5649';
const SPOTIFY_CLIENT_SECRET = 'efc644034a9c4f49aadfa3d75047effc';
const DB_PATH = '/home/ahmed/vexo-1/data/musicbot.db';
const COOLDOWN_HOURS = 8;
const LIBRARY_RATIO = 0.2; // 1 in 5 from library

// Deezer Genre IDs
const DEEZER_GENRES = {
    pop: 132, rock: 152, hiphop: 116, electro: 106, rnb: 165,
    jazz: 129, classical: 98, reggae: 144, latin: 197, metal: 464,
    country: 84, soul: 169, indie: 85, alternative: 85
};

// Deezer Radio IDs
const DEEZER_RADIOS = {
    hits: 37151, eighties: 38305, nineties: 36881, indie: 30771,
    chill: 37825, workout: 37233, party: 37227, focus: 37787
};

// ========================================
// SPOTIFY AUTH
// ========================================
let spotifyToken = null;

async function getSpotifyToken() {
    if (spotifyToken) return spotifyToken;
    const response = await fetch('https://accounts.spotify.com/api/token', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + Buffer.from(`${SPOTIFY_CLIENT_ID}:${SPOTIFY_CLIENT_SECRET}`).toString('base64')
        },
        body: 'grant_type=client_credentials'
    });
    const data = await response.json();
    spotifyToken = data.access_token;
    return spotifyToken;
}

async function spotifyFetch(endpoint) {
    const token = await getSpotifyToken();
    try {
        const response = await fetch(`https://api.spotify.com/v1${endpoint}`, {
            headers: { 'Authorization': `Bearer ${token}` }
        });
        if (!response.ok) return null;
        return response.json();
    } catch { return null; }
}

// ========================================
// DEEZER API FUNCTIONS
// ========================================
async function deezerFetch(endpoint) {
    try {
        const response = await fetch(`https://api.deezer.com${endpoint}`);
        if (!response.ok) return null;
        return response.json();
    } catch { return null; }
}

async function deezerSearch(query) {
    const data = await deezerFetch(`/search?q=${encodeURIComponent(query)}&limit=1`);
    return data?.data?.[0] || null;
}

async function deezerTrack(trackId) {
    return await deezerFetch(`/track/${trackId}`);
}

async function deezerArtistTop(artistId, limit = 10) {
    const data = await deezerFetch(`/artist/${artistId}/top?limit=${limit}`);
    return data?.data || [];
}

async function deezerArtistRelated(artistId, limit = 10) {
    const data = await deezerFetch(`/artist/${artistId}/related?limit=${limit}`);
    return data?.data || [];
}

async function deezerChart(limit = 50) {
    const data = await deezerFetch(`/chart/0/tracks?limit=${limit}`);
    return data?.data || [];
}

async function deezerGenreArtists(genreId, limit = 10) {
    const data = await deezerFetch(`/genre/${genreId}/artists?limit=${limit}`);
    return data?.data || [];
}

async function deezerRadioTracks(radioId, limit = 25) {
    const data = await deezerFetch(`/radio/${radioId}/tracks?limit=${limit}`);
    return data?.data || [];
}

async function deezerArtistRadio(artistId) {
    // Artist "radio" - mix based on the artist
    const data = await deezerFetch(`/artist/${artistId}/radio?limit=20`);
    return data?.data || [];
}

async function deezerPlaylist(playlistId) {
    const data = await deezerFetch(`/playlist/${playlistId}/tracks?limit=30`);
    return data?.data || [];
}

// ========================================
// SPOTIFY HELPERS
// ========================================
async function searchSpotifyTrack(title, artist) {
    const cleanTitle = title.replace(/\s*\(.*?\)\s*/g, '').trim();
    const query = encodeURIComponent(`${cleanTitle} ${artist}`);
    const data = await spotifyFetch(`/search?q=${query}&type=track&limit=1`);
    return data?.tracks?.items?.[0] || null;
}

async function getSpotifyArtist(artistId) {
    return await spotifyFetch(`/artists/${artistId}`);
}

async function spotifyRelatedArtists(artistId) {
    const data = await spotifyFetch(`/artists/${artistId}/related-artists`);
    return data?.artists || [];
}

// ========================================
// HELPER: Add track to pool with dedup
// ========================================
function addToPool(pool, track, source, recentTitles, seenIds) {
    const key = `${track.title?.toLowerCase() || ''}|${(track.artist?.name || track.artist_name || '').toLowerCase()}`;
    if (recentTitles.has(key)) return false;
    if (seenIds.has(track.id)) return false;
    
    seenIds.add(track.id);
    pool.push({
        song_id: `${source}_${track.id}`,
        title: track.title,
        artist_name: track.artist?.name || track.artist_name || 'Unknown',
        popularity: Math.round((track.rank || 500000) / 10000),
        genres: [],
        bpm: null,
        source
    });
    return true;
}

// ========================================
// MAIN
// ========================================
async function main() {
    console.log('='.repeat(75));
    console.log('VEXO DISCOVERY ENGINE V8 - MAXIMUM SOURCES');
    console.log('='.repeat(75) + '\n');
    
    const db = new Database(DB_PATH);
    
    // ========================================
    // PHASE 0: PLAYBACK HISTORY
    // ========================================
    console.log('PHASE 0: PLAYBACK HISTORY (8-Hour Cooldown)');
    console.log('-'.repeat(75));
    
    const eightHoursAgo = Date.now() - (COOLDOWN_HOURS * 60 * 60 * 1000);
    const recentHistory = db.query(`
        SELECT DISTINCT s.title, s.artist_name
        FROM playback_history ph
        JOIN songs s ON ph.song_id = s.id
        WHERE ph.played_at > ?
    `).all(eightHoursAgo);
    
    const recentTitles = new Set(recentHistory.map(h => `${h.title.toLowerCase()}|${h.artist_name.toLowerCase()}`));
    console.log(`  Tracks on cooldown: ${recentTitles.size}\n`);
    
    // ========================================
    // PHASE 1: LIBRARY ENRICHMENT
    // ========================================
    console.log('PHASE 1: LIBRARY ENRICHMENT');
    console.log('-'.repeat(75));
    
    const library = db.query(`
        SELECT s.id as song_id, s.title, s.artist_name, u.username as added_by, u.id as user_id
        FROM song_library_entries sle
        JOIN songs s ON sle.song_id = s.id
        JOIN users u ON sle.user_id = u.id
    `).all();
    
    const enrichedTracks = [];
    const artistCache = new Map();
    const deezerArtistIds = new Set();
    const spotifyArtistIds = new Set();
    
    for (const track of library) {
        const shortTitle = track.title.substring(0, 22).padEnd(22);
        process.stdout.write(`  ${shortTitle} `);
        
        // Spotify
        const spotifyTrack = await searchSpotifyTrack(track.title, track.artist_name);
        let genres = [], popularity = 50;
        
        if (spotifyTrack) {
            popularity = spotifyTrack.popularity;
            const artistId = spotifyTrack.artists[0].id;
            spotifyArtistIds.add(artistId);
            
            let artistInfo = artistCache.get(artistId);
            if (!artistInfo) {
                artistInfo = await getSpotifyArtist(artistId);
                artistCache.set(artistId, artistInfo);
            }
            genres = artistInfo?.genres || [];
            process.stdout.write(`SP:${String(popularity).padStart(2)} `);
        } else {
            process.stdout.write(`SP:-- `);
        }
        
        // Deezer
        const deezerResult = await deezerSearch(`${track.title} ${track.artist_name}`);
        let bpm = null, deezerArtistId = null;
        
        if (deezerResult) {
            deezerArtistId = deezerResult.artist?.id;
            if (deezerArtistId) deezerArtistIds.add(deezerArtistId);
            const fullInfo = await deezerTrack(deezerResult.id);
            bpm = fullInfo?.bpm || null;
            process.stdout.write(`DZ:${bpm ? `${bpm.toFixed(0)}bpm` : '--'}`);
        } else {
            process.stdout.write(`DZ:--`);
        }
        
        console.log('');
        
        enrichedTracks.push({
            ...track, popularity, genres, bpm, deezerArtistId, source: 'library'
        });
    }
    
    console.log(`\n  Library: ${enrichedTracks.length} | Spotify Artists: ${spotifyArtistIds.size} | Deezer Artists: ${deezerArtistIds.size}\n`);
    
    // ========================================
    // PHASE 2: MEGA DISCOVERY POOL
    // ========================================
    console.log('PHASE 2: BUILDING MEGA DISCOVERY POOL');
    console.log('-'.repeat(75));
    
    const discoveryPool = [];
    const seenIds = new Set();
    
    // 2a. DEEZER RELATED ARTISTS
    console.log('  [A] Deezer Related Artists...');
    let relatedCount = 0;
    for (const artistId of [...deezerArtistIds].slice(0, 5)) {
        const related = await deezerArtistRelated(artistId, 8);
        for (const ra of related.slice(0, 4)) {
            const top = await deezerArtistTop(ra.id, 5);
            for (const t of top) {
                if (addToPool(discoveryPool, t, 'dz_related', recentTitles, seenIds)) relatedCount++;
            }
        }
    }
    console.log(`      Added: ${relatedCount}`);
    
    // 2b. DEEZER ARTIST RADIO (Similar tracks)
    console.log('  [B] Deezer Artist Radio...');
    let artistRadioCount = 0;
    for (const artistId of [...deezerArtistIds].slice(0, 3)) {
        const radio = await deezerArtistRadio(artistId);
        for (const t of radio) {
            if (addToPool(discoveryPool, t, 'dz_artist_radio', recentTitles, seenIds)) artistRadioCount++;
        }
    }
    console.log(`      Added: ${artistRadioCount}`);
    
    // 2c. SPOTIFY RELATED ARTISTS
    console.log('  [C] Spotify Related Artists...');
    let spotifyRelCount = 0;
    for (const artistId of [...spotifyArtistIds].slice(0, 3)) {
        const related = await spotifyRelatedArtists(artistId);
        for (const ra of related.slice(0, 5)) {
            // Search on Deezer to get track data
            const topSearch = await deezerSearch(ra.name);
            if (topSearch) {
                const top = await deezerArtistTop(topSearch.artist.id, 3);
                for (const t of top) {
                    if (addToPool(discoveryPool, t, 'sp_related', recentTitles, seenIds)) spotifyRelCount++;
                }
            }
        }
    }
    console.log(`      Added: ${spotifyRelCount}`);
    
    // 2d. GLOBAL CHARTS
    console.log('  [D] Global Charts...');
    let chartCount = 0;
    const charts = await deezerChart(40);
    for (const t of charts) {
        if (addToPool(discoveryPool, t, 'dz_chart', recentTitles, seenIds)) chartCount++;
    }
    console.log(`      Added: ${chartCount}`);
    
    // 2e. GENRE-BASED DISCOVERY (based on library genres)
    console.log('  [E] Genre-Based Discovery...');
    const genreCounts = {};
    enrichedTracks.forEach(t => t.genres.forEach(g => { genreCounts[g] = (genreCounts[g] || 0) + 1; }));
    const topGenres = Object.entries(genreCounts).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([g]) => g);
    
    let genreCount = 0;
    for (const genre of topGenres) {
        const genreKey = genre.split(' ')[0].toLowerCase().replace(/[^a-z]/g, '');
        const genreId = DEEZER_GENRES[genreKey] || DEEZER_GENRES.pop;
        const artists = await deezerGenreArtists(genreId, 5);
        for (const a of artists) {
            const top = await deezerArtistTop(a.id, 3);
            for (const t of top) {
                if (addToPool(discoveryPool, t, 'dz_genre', recentTitles, seenIds)) genreCount++;
            }
        }
    }
    console.log(`      Added: ${genreCount} (from ${topGenres.join(', ')})`);
    
    // 2f. CURATED RADIOS
    console.log('  [F] Curated Radios...');
    let radioCount = 0;
    const radioIds = [DEEZER_RADIOS.hits, DEEZER_RADIOS.indie, DEEZER_RADIOS.chill];
    for (const radioId of radioIds) {
        const tracks = await deezerRadioTracks(radioId, 15);
        for (const t of tracks) {
            if (addToPool(discoveryPool, t, 'dz_radio', recentTitles, seenIds)) radioCount++;
        }
    }
    console.log(`      Added: ${radioCount}`);
    
    // 2g. WILDCARD - Random Genre Exploration
    console.log('  [G] Wildcard Exploration...');
    let wildcardCount = 0;
    const wildcardGenres = ['jazz', 'classical', 'reggae', 'latin', 'metal', 'country'];
    const randomGenre = wildcardGenres[Math.floor(Math.random() * wildcardGenres.length)];
    const wcArtists = await deezerGenreArtists(DEEZER_GENRES[randomGenre] || 132, 5);
    for (const a of wcArtists) {
        const top = await deezerArtistTop(a.id, 3);
        for (const t of top) {
            if (addToPool(discoveryPool, t, 'wildcard', recentTitles, seenIds)) wildcardCount++;
        }
    }
    console.log(`      Added: ${wildcardCount} (${randomGenre} wildcard)`);
    
    // 2h. ERA-BASED (80s, 90s)
    console.log('  [H] Era-Based (80s/90s)...');
    let eraCount = 0;
    for (const radioId of [DEEZER_RADIOS.eighties, DEEZER_RADIOS.nineties]) {
        const tracks = await deezerRadioTracks(radioId, 10);
        for (const t of tracks) {
            if (addToPool(discoveryPool, t, 'dz_era', recentTitles, seenIds)) eraCount++;
        }
    }
    console.log(`      Added: ${eraCount}`);
    
    console.log(`\n  TOTAL DISCOVERY POOL: ${discoveryPool.length} unique tracks\n`);
    
    // ========================================
    // PHASE 3: USER PROFILES
    // ========================================
    console.log('PHASE 3: USER PROFILES');
    console.log('-'.repeat(75));
    
    const users = db.query("SELECT id, username FROM users WHERE opted_out = 0").all();
    const userProfiles = {};
    
    users.forEach(u => {
        const userTracks = enrichedTracks.filter(t => t.user_id === u.id);
        if (userTracks.length === 0) return;
        
        const genreVector = {};
        userTracks.forEach(t => t.genres.forEach(g => { genreVector[g] = (genreVector[g] || 0) + 1; }));
        
        const withBpm = userTracks.filter(t => t.bpm && t.bpm > 0);
        const avgBpm = withBpm.length ? withBpm.reduce((s, t) => s + t.bpm, 0) / withBpm.length : 120;
        
        userProfiles[u.id] = { username: u.username, avgBpm, genreVector };
        
        const topG = Object.entries(genreVector).sort((a, b) => b[1] - a[1]).slice(0, 2).map(([g]) => g);
        console.log(`  ${u.username.padEnd(14)} BPM:${avgBpm.toFixed(0).padStart(3)} | ${topG.join(', ') || '-'}`);
    });
    
    // ========================================
    // PHASE 4: DISCOVERY SIMULATION
    // ========================================
    console.log('\n\nPHASE 4: DISCOVERY SIMULATION (20 turns)');
    console.log('-'.repeat(75));
    
    function genreSim(trackGenres, profileGenres) {
        if (!trackGenres.length || !Object.keys(profileGenres).length) return 0.3;
        let matches = 0;
        trackGenres.forEach(g => { if (profileGenres[g]) matches++; });
        return matches / Math.max(trackGenres.length, Object.keys(profileGenres).length);
    }
    
    const availableLibrary = enrichedTracks.filter(t => {
        const key = `${t.title.toLowerCase()}|${t.artist_name.toLowerCase()}`;
        return !recentTitles.has(key);
    });
    
    console.log(`  Library available: ${availableLibrary.length}/${enrichedTracks.length}`);
    console.log(`  Discovery pool: ${discoveryPool.length}\n`);
    
    const activeUsers = Object.values(userProfiles);
    const played = new Set();
    let libraryPlays = 0;
    const sourceCounts = {};
    
    for (let turn = 1; turn <= 20; turn++) {
        const profile = activeUsers[(turn - 1) % activeUsers.length];
        const shouldPlayLibrary = (turn % 5 === 0);
        
        process.stdout.write(`  [${String(turn).padStart(2)}] ${profile.username.padEnd(11)} `);
        
        const pool = shouldPlayLibrary && availableLibrary.filter(t => !played.has(t.song_id)).length > 0 
            ? availableLibrary 
            : discoveryPool;
        
        const scored = pool
            .filter(t => !played.has(t.song_id))
            .map(t => {
                const genre = genreSim(t.genres, profile.genreVector);
                const pop = (t.popularity || 50) / 100;
                const wildcard = t.source === 'wildcard' ? 0.15 : 0;
                const score = genre * 0.4 + pop * 0.4 + wildcard + Math.random() * 0.1;
                return { ...t, score };
            })
            .sort((a, b) => b.score - a.score);
        
        if (scored.length) {
            const pick = scored[0];
            played.add(pick.song_id);
            
            const srcKey = pick.source;
            sourceCounts[srcKey] = (sourceCounts[srcKey] || 0) + 1;
            if (pick.source === 'library') libraryPlays++;
            
            const srcLabel = pick.source.replace('dz_', '').replace('sp_', '').toUpperCase().substring(0, 8);
            console.log(`-> ${(pick.title || '?').substring(0, 24).padEnd(24)} - ${(pick.artist_name || '?').substring(0, 14)}`);
            console.log(`                        [${srcLabel.padEnd(8)}]`);
        } else {
            console.log('No candidates');
        }
    }
    
    // ========================================
    // STATS
    // ========================================
    console.log('\n' + '='.repeat(75));
    console.log('DISCOVERY STATS');
    console.log('='.repeat(75));
    console.log(`  Library plays: ${libraryPlays}/20 (${(libraryPlays/20*100).toFixed(0)}%)`);
    console.log(`  Discovery plays: ${20 - libraryPlays}/20 (${((20-libraryPlays)/20*100).toFixed(0)}%)\n`);
    console.log('  Source Breakdown:');
    Object.entries(sourceCounts).sort((a, b) => b[1] - a[1]).forEach(([src, cnt]) => {
        console.log(`    ${src.padEnd(18)} : ${cnt}`);
    });
    
    db.close();
    console.log('\n' + '='.repeat(75));
    console.log('Discovery Complete');
    console.log('='.repeat(75));
}

main().catch(console.error);
