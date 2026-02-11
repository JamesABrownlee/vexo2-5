import { getUserDetail } from '@/lib/db';
import { notFound } from 'next/navigation';
import Link from 'next/link';

export const dynamic = 'force-dynamic';
import { ArrowLeft, TrendingUp, Heart, ListMusic, Sparkles, Clock, Music2 } from 'lucide-react';

interface PageProps {
    params: Promise<{ id: string }>;
}

interface UserData {
    user: {
        id: number;
        username: string;
        created_at: string;
        last_active: string;
    };
    stats: {
        plays: number;
        reactions: number;
        playlists: number;
    };
    recentSongs: Array<{
        title: string;
        artist_name: string;
        played_at: string;
        discovery_source: string;
    }>;
    likedSongs: Array<{
        title: string;
        artist_name: string;
        reaction: string;
    }>;
    preferences: Array<{
        preference_type: string;
        preference_key: string;
        affinity_score: number;
    }>;
    importedPlaylists: Array<{
        platform: string;
        playlist_name: string;
        track_count: number;
        imported_at: string;
    }>;
}

function PreferenceBar({ label, value, maxValue, color }: { label: string; value: number; maxValue: number; color: string }) {
    const percentage = maxValue > 0 ? (value / maxValue) * 100 : 0;
    return (
        <div className="space-y-1">
            <div className="flex justify-between text-sm">
                <span className="text-zinc-400 truncate max-w-[150px]">{label}</span>
                <span className="text-zinc-500">{value.toFixed(1)}</span>
            </div>
            <div className="h-2 bg-white/[0.04] rounded-full overflow-hidden">
                <div className={`h-full ${color} rounded-full transition-all`} style={{ width: `${percentage}%` }} />
            </div>
        </div>
    );
}

export default async function UserDetailPage({ params }: PageProps) {
    const { id } = await params;

    let userData: UserData | null = null;

    try {
        userData = (await getUserDetail(id) as unknown) as UserData;
    } catch (error) {
        console.error('Failed to fetch user:', error);
    }

    if (!userData) {
        notFound();
    }

    const { user, stats, recentSongs, likedSongs, preferences, importedPlaylists } = userData;

    // Group preferences by type
    const genrePrefs = preferences.filter(p => p.preference_type === 'genre').slice(0, 5);
    const artistPrefs = preferences.filter(p => p.preference_type === 'artist').slice(0, 5);
    const maxAffinity = Math.max(...preferences.map(p => p.affinity_score), 1);

    // Format dates nicely
    const joinDate = new Date(user.created_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    const lastActive = new Date(user.last_active).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    const isActiveToday = new Date(user.last_active).toDateString() === new Date().toDateString();

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center gap-4">
                <Link href="/users" className="p-2 rounded-xl hover:bg-white/[0.08] transition-colors">
                    <ArrowLeft className="w-5 h-5 text-zinc-400" />
                </Link>
                <span className="text-zinc-600">Back to Users</span>
            </div>

            {/* Profile Card */}
            <div className="bento-card">
                <div className="flex items-center gap-6">
                    <div className="w-20 h-20 rounded-full bg-gradient-to-br from-violet-500 to-pink-500 flex items-center justify-center text-3xl font-bold">
                        {user.username?.[0]?.toUpperCase() || '?'}
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-white">{user.username}</h1>
                        <p className="text-zinc-500">
                            Member since {joinDate}
                            <span className="mx-2">•</span>
                            <span className={isActiveToday ? 'text-green-400' : 'text-zinc-500'}>
                                {isActiveToday ? 'Active Today' : `Last seen ${lastActive}`}
                            </span>
                        </p>
                    </div>
                </div>
            </div>

            {/* Stats Cards */}
            <div className="grid grid-cols-4 gap-4">
                <div className="bento-card text-center">
                    <div className="w-10 h-10 rounded-xl bg-violet-500/20 flex items-center justify-center mx-auto mb-2">
                        <TrendingUp className="w-5 h-5 text-violet-400" />
                    </div>
                    <p className="text-2xl font-bold text-white">{stats.plays}</p>
                    <p className="text-sm text-zinc-500">Total Plays</p>
                </div>
                <div className="bento-card text-center">
                    <div className="w-10 h-10 rounded-xl bg-pink-500/20 flex items-center justify-center mx-auto mb-2">
                        <Heart className="w-5 h-5 text-pink-400" />
                    </div>
                    <p className="text-2xl font-bold text-white">{stats.reactions}</p>
                    <p className="text-sm text-zinc-500">Reactions</p>
                </div>
                <div className="bento-card text-center">
                    <div className="w-10 h-10 rounded-xl bg-blue-500/20 flex items-center justify-center mx-auto mb-2">
                        <ListMusic className="w-5 h-5 text-blue-400" />
                    </div>
                    <p className="text-2xl font-bold text-white">{stats.playlists}</p>
                    <p className="text-sm text-zinc-500">Playlists</p>
                </div>
                <div className="bento-card text-center">
                    <div className="w-10 h-10 rounded-xl bg-green-500/20 flex items-center justify-center mx-auto mb-2">
                        <Sparkles className="w-5 h-5 text-green-400" />
                    </div>
                    <p className="text-2xl font-bold text-white">{preferences.length}</p>
                    <p className="text-sm text-zinc-500">Preferences</p>
                </div>
            </div>

            {/* Main Content */}
            <div className="grid grid-cols-2 gap-6">
                {/* Left Column */}
                <div className="space-y-4">
                    {/* Recent Songs */}
                    <div className="bento-card">
                        <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
                            <Clock className="w-5 h-5 text-violet-500" />
                            Recent Songs
                            <span className="text-sm text-zinc-500 font-normal ml-auto">{recentSongs.length} songs</span>
                        </h3>
                        <div className="space-y-2 max-h-[300px] overflow-y-auto">
                            {recentSongs.map((song, i) => (
                                <div key={i} className="flex items-center gap-3 p-2 rounded-lg hover:bg-white/[0.04] transition-colors">
                                    <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-violet-500/20 to-pink-500/20 flex items-center justify-center">
                                        <Music2 className="w-4 h-4 text-violet-400" />
                                    </div>
                                    <div className="flex-1 min-w-0">
                                        <p className="text-sm text-white truncate">{song.title}</p>
                                        <p className="text-xs text-zinc-500 truncate">{song.artist_name}</p>
                                    </div>
                                    <span className={`text-xs px-2 py-0.5 rounded-full ${song.discovery_source === 'user_request'
                                        ? 'bg-blue-500/20 text-blue-400'
                                        : 'bg-green-500/20 text-green-400'
                                        }`}>
                                        {song.discovery_source === 'user_request' ? 'Request' : 'Discovery'}
                                    </span>
                                </div>
                            ))}
                            {recentSongs.length === 0 && (
                                <p className="text-sm text-zinc-600 text-center py-4">No recent songs</p>
                            )}
                        </div>
                    </div>

                    {/* Liked Songs */}
                    <div className="bento-card">
                        <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
                            <Heart className="w-5 h-5 text-pink-500" />
                            Liked Songs
                            <span className="text-sm text-zinc-500 font-normal ml-auto">{likedSongs.length} songs</span>
                        </h3>
                        <div className="space-y-2 max-h-[300px] overflow-y-auto">
                            {likedSongs.map((song, i) => (
                                <div key={i} className="flex items-center gap-3 p-2 rounded-lg hover:bg-white/[0.04] transition-colors">
                                    <span className="text-lg">{song.reaction === 'like' ? '👍' : song.reaction === 'love' ? '❤️' : '🎵'}</span>
                                    <div className="flex-1 min-w-0">
                                        <p className="text-sm text-white truncate">{song.title}</p>
                                        <p className="text-xs text-zinc-500 truncate">{song.artist_name}</p>
                                    </div>
                                </div>
                            ))}
                            {likedSongs.length === 0 && (
                                <p className="text-sm text-zinc-600 text-center py-4">No liked songs yet</p>
                            )}
                        </div>
                    </div>
                </div>

                {/* Right Column */}
                <div className="space-y-4">
                    {/* Top Genres */}
                    <div className="bento-card">
                        <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
                            <Sparkles className="w-5 h-5 text-green-500" />
                            Top Genres
                        </h3>
                        <div className="space-y-3">
                            {genrePrefs.map((pref, i) => (
                                <PreferenceBar
                                    key={i}
                                    label={pref.preference_key}
                                    value={pref.affinity_score}
                                    maxValue={maxAffinity}
                                    color="bg-violet-500"
                                />
                            ))}
                            {genrePrefs.length === 0 && (
                                <p className="text-sm text-zinc-600 text-center py-4">No genre preferences yet</p>
                            )}
                        </div>
                    </div>

                    {/* Top Artists */}
                    <div className="bento-card">
                        <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
                            <Music2 className="w-5 h-5 text-pink-500" />
                            Top Artists
                        </h3>
                        <div className="space-y-3">
                            {artistPrefs.map((pref, i) => (
                                <PreferenceBar
                                    key={i}
                                    label={pref.preference_key}
                                    value={pref.affinity_score}
                                    maxValue={maxAffinity}
                                    color="bg-pink-500"
                                />
                            ))}
                            {artistPrefs.length === 0 && (
                                <p className="text-sm text-zinc-600 text-center py-4">No artist preferences yet</p>
                            )}
                        </div>
                    </div>

                    {/* Imported Playlists */}
                    {importedPlaylists.length > 0 && (
                        <div className="bento-card">
                            <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
                                <ListMusic className="w-5 h-5 text-blue-500" />
                                Imported Playlists
                            </h3>
                            <div className="space-y-2">
                                {importedPlaylists.map((playlist, i) => (
                                    <div key={i} className="flex items-center justify-between p-2 rounded-lg hover:bg-white/[0.04] transition-colors">
                                        <div>
                                            <p className="text-sm text-white">{playlist.playlist_name}</p>
                                            <p className="text-xs text-zinc-500">{playlist.platform} • {playlist.track_count} tracks</p>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
