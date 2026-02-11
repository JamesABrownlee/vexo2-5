import {
  Music2, Users, Play, Heart, Disc3, TrendingUp, Clock,
  Sparkles, BarChart3, Zap, Award, Radio
} from 'lucide-react';
import {
  getTotalStats, getTopSongs, getTopUsers, getTopLikedGenres,
  getTopLikedArtists, getTopLikedSongs, getDiscoveryBreakdown,
  getGenreDistribution, getUsefulUsers, getRecentHistory
} from '@/lib/db';

export const dynamic = 'force-dynamic';

// Stat Card Component
function StatCard({
  icon: Icon,
  label,
  value,
  trend,
  color = 'violet'
}: {
  icon: React.ElementType;
  label: string;
  value: string | number;
  trend?: string;
  color?: 'violet' | 'pink' | 'blue' | 'green' | 'orange';
}) {
  const colorClasses = {
    violet: 'from-violet-500/20 to-violet-500/5 text-violet-400',
    pink: 'from-pink-500/20 to-pink-500/5 text-pink-400',
    blue: 'from-blue-500/20 to-blue-500/5 text-blue-400',
    green: 'from-green-500/20 to-green-500/5 text-green-400',
    orange: 'from-orange-500/20 to-orange-500/5 text-orange-400',
  };

  return (
    <div className="bento-card group hover:scale-[1.02] transition-transform">
      <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${colorClasses[color]} flex items-center justify-center mb-3`}>
        <Icon className="w-6 h-6" />
      </div>
      <p className="text-3xl font-bold text-white">{value}</p>
      <p className="text-sm text-zinc-500 mt-1">{label}</p>
      {trend && (
        <p className="text-xs text-green-500 mt-2 flex items-center gap-1">
          <TrendingUp className="w-3 h-3" />
          {trend}
        </p>
      )}
    </div>
  );
}

import NowPlayingCard from '@/components/NowPlayingCard';

// Top Songs List
function TopSongsList({ songs }: { songs: Array<{ title: string; artist: string; plays: number; likes: number }> }) {
  return (
    <div className="bento-card">
      <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
        <Disc3 className="w-5 h-5 text-violet-500" />
        Top Songs
      </h3>
      <div className="space-y-2">
        {songs.slice(0, 5).map((song, i) => (
          <div key={i} className="flex items-center gap-3 p-2 rounded-lg hover:bg-white/[0.04] transition-colors group">
            <span className="text-lg font-bold text-zinc-600 w-6">{i + 1}</span>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-white truncate group-hover:text-violet-400 transition-colors">
                {song.title}
              </p>
              <p className="text-xs text-zinc-500 truncate">{song.artist}</p>
            </div>
            <div className="flex items-center gap-3 text-xs text-zinc-500">
              <span className="flex items-center gap-1">
                <Play className="w-3 h-3" />
                {song.plays}
              </span>
              <span className="flex items-center gap-1 text-pink-500">
                <Heart className="w-3 h-3" />
                {song.likes}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Top Users List
function TopUsersList({ users }: { users: Array<{ id: string; username: string; plays: number; reactions: number }> }) {
  return (
    <div className="bento-card">
      <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
        <Users className="w-5 h-5 text-pink-500" />
        Most Active Users
      </h3>
      <div className="space-y-2">
        {users.slice(0, 5).map((user, i) => (
          <a
            key={i}
            href={`/users/${user.id}`}
            className="flex items-center gap-3 p-2 rounded-lg hover:bg-white/[0.04] transition-colors group"
          >
            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-violet-500 to-pink-500 flex items-center justify-center text-sm font-bold">
              {user.username?.[0]?.toUpperCase() || '?'}
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium text-white truncate group-hover:text-violet-400 transition-colors">
                {user.username}
              </p>
            </div>
            <div className="flex items-center gap-3 text-xs text-zinc-500">
              <span>{user.plays} plays</span>
              <span className="text-pink-500">{user.reactions} ❤️</span>
            </div>
          </a>
        ))}
      </div>
    </div>
  );
}

// Genre Distribution Chart
function GenreChart({ genres }: { genres: Array<{ name: string; plays: number }> }) {
  const maxPlays = Math.max(...genres.map(g => g.plays), 1);
  const colors = ['bg-violet-500', 'bg-pink-500', 'bg-blue-500', 'bg-green-500', 'bg-orange-500'];

  return (
    <div className="bento-card">
      <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
        <BarChart3 className="w-5 h-5 text-blue-500" />
        Most Played Genres
      </h3>
      <div className="space-y-3">
        {genres.slice(0, 5).map((genre, i) => (
          <div key={i}>
            <div className="flex justify-between text-sm mb-1">
              <span className="text-white capitalize">{genre.name}</span>
              <span className="text-zinc-500">{genre.plays}</span>
            </div>
            <div className="h-2 bg-white/[0.04] rounded-full overflow-hidden">
              <div
                className={`h-full ${colors[i % colors.length]} rounded-full transition-all`}
                style={{ width: `${(genre.plays / maxPlays) * 100}%` }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Discovery Source Breakdown
function DiscoveryBreakdown({ sources }: { sources: Array<{ source: string; count: number }> }) {
  const total = sources.reduce((sum, s) => sum + s.count, 0);
  const sourceLabels: Record<string, { label: string; color: string }> = {
    'user_request': { label: 'Requested', color: 'bg-blue-500' },
    'similar': { label: 'Similar', color: 'bg-violet-500' },
    'same_artist': { label: 'Same Artist', color: 'bg-pink-500' },
    'wildcard': { label: 'Discovery', color: 'bg-green-500' },
  };

  return (
    <div className="bento-card">
      <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
        <Sparkles className="w-5 h-5 text-green-500" />
        How Songs Were Found
      </h3>
      <div className="flex gap-1 h-4 rounded-full overflow-hidden mb-4">
        {sources.map((source, i) => {
          const info = sourceLabels[source.source] || { label: source.source, color: 'bg-zinc-500' };
          return (
            <div
              key={i}
              className={`${info.color} transition-all`}
              style={{ width: `${(source.count / total) * 100}%` }}
              title={`${info.label}: ${source.count}`}
            />
          );
        })}
      </div>
      <div className="grid grid-cols-2 gap-2">
        {sources.map((source, i) => {
          const info = sourceLabels[source.source] || { label: source.source, color: 'bg-zinc-500' };
          return (
            <div key={i} className="flex items-center gap-2 text-sm">
              <div className={`w-3 h-3 rounded-full ${info.color}`} />
              <span className="text-zinc-400">{info.label}</span>
              <span className="text-zinc-600 ml-auto">{Math.round((source.count / total) * 100)}%</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Helpful Users (users whose requests got liked by others)
function HelpfulUsers({ users }: { users: Array<{ username: string; score: number }> }) {
  return (
    <div className="bento-card">
      <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
        <Award className="w-5 h-5 text-orange-500" />
        Top Curators
      </h3>
      <p className="text-xs text-zinc-500 mb-3">Users whose requests got the most likes from others</p>
      <div className="space-y-2">
        {users.slice(0, 5).map((user, i) => (
          <div key={i} className="flex items-center gap-3 p-2 rounded-lg hover:bg-white/[0.04] transition-colors">
            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-orange-500 to-yellow-500 flex items-center justify-center text-sm font-bold">
              {user.username?.[0]?.toUpperCase() || '?'}
            </div>
            <span className="text-sm text-white flex-1">{user.username}</span>
            <span className="text-sm text-orange-400">{user.score} 👍</span>
          </div>
        ))}
        {users.length === 0 && (
          <p className="text-sm text-zinc-600 text-center py-4">No data yet</p>
        )}
      </div>
    </div>
  );
}

// Recent Activity
function RecentActivity({ history }: { history: Array<{ title: string; artist_name: string; played_at: string }> }) {
  const formatTime = (dateStr: string) => {
    const date = new Date(dateStr);
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `${hours}h ago`;
    return date.toLocaleDateString();
  };

  return (
    <div className="bento-card">
      <h3 className="text-lg font-semibold flex items-center gap-2 mb-4">
        <Clock className="w-5 h-5 text-cyan-500" />
        Recent Activity
      </h3>
      <div className="space-y-2">
        {history.slice(0, 5).map((item, i) => (
          <div key={i} className="flex items-center gap-3 p-2 rounded-lg hover:bg-white/[0.04] transition-colors">
            <Radio className="w-4 h-4 text-zinc-500" />
            <div className="flex-1 min-w-0">
              <p className="text-sm text-white truncate">{item.title}</p>
              <p className="text-xs text-zinc-500 truncate">{item.artist_name}</p>
            </div>
            <span className="text-xs text-zinc-600">{formatTime(item.played_at)}</span>
          </div>
        ))}
        {history.length === 0 && (
          <p className="text-sm text-zinc-600 text-center py-4">No recent plays</p>
        )}
      </div>
    </div>
  );
}

// Insight Cards
function InsightCard({
  icon: Icon,
  label,
  value,
  subtext,
  color
}: {
  icon: React.ElementType;
  label: string;
  value: string;
  subtext?: string;
  color: string;
}) {
  return (
    <div className="bento-card text-center group hover:scale-[1.02] transition-transform">
      <div className={`w-10 h-10 rounded-xl ${color} flex items-center justify-center mx-auto mb-2`}>
        <Icon className="w-5 h-5" />
      </div>
      <p className="text-xs text-zinc-500 uppercase tracking-wider">{label}</p>
      <p className="text-lg font-bold text-white mt-1 truncate">{value}</p>
      {subtext && <p className="text-xs text-zinc-600 mt-1">{subtext}</p>}
    </div>
  );
}

export default async function DashboardPage() {
  // Fetch all data
  let stats = { totalPlays: 0, totalSongs: 0, totalUsers: 0 };
  let topSongs: Array<{ title: string; artist: string; plays: number; likes: number }> = [];
  let topUsers: Array<{ id: string; username: string; plays: number; reactions: number }> = [];
  let topGenres: Array<{ name: string; likes: number }> = [];
  let topArtists: Array<{ name: string; likes: number }> = [];
  let topLikedSongs: Array<{ title: string; artist: string; likes: number }> = [];
  let discoveryBreakdown: Array<{ source: string; count: number }> = [];
  let genreDistribution: Array<{ name: string; plays: number }> = [];
  let helpfulUsers: Array<{ username: string; score: number }> = [];
  let recentHistory: Array<{ title: string; artist_name: string; played_at: string }> = [];

  try {
    stats = await getTotalStats();
    topSongs = await getTopSongs(10) as typeof topSongs;
    topUsers = await getTopUsers(10) as typeof topUsers;
    topGenres = await getTopLikedGenres(5) as typeof topGenres;
    topArtists = await getTopLikedArtists(5) as typeof topArtists;
    topLikedSongs = await getTopLikedSongs(5) as typeof topLikedSongs;
    discoveryBreakdown = await getDiscoveryBreakdown() as typeof discoveryBreakdown;
    genreDistribution = await getGenreDistribution(5) as typeof genreDistribution;
    helpfulUsers = await getUsefulUsers(5) as typeof helpfulUsers;
    recentHistory = await getRecentHistory(5) as typeof recentHistory;
  } catch (error) {
    console.error('Database not available:', error);
  }

  // Get top insights
  const topGenre = topGenres[0]?.name || 'Unknown';
  const topArtist = topArtists[0]?.name || 'Unknown';
  const topSong = topLikedSongs[0]?.title || 'Unknown';

  return (
    <div className="space-y-6">
      {/* Stats Row */}
      <div className="grid grid-cols-4 gap-4">
        <StatCard
          icon={Play}
          label="Total Plays"
          value={stats.totalPlays.toLocaleString()}
          color="violet"
        />
        <StatCard
          icon={Music2}
          label="Songs in Library"
          value={stats.totalSongs.toLocaleString()}
          color="pink"
        />
        <StatCard
          icon={Users}
          label="Active Users"
          value={stats.totalUsers.toLocaleString()}
          color="blue"
        />
        <StatCard
          icon={Heart}
          label="Total Reactions"
          value={topUsers.reduce((sum, u) => sum + u.reactions, 0).toLocaleString()}
          color="green"
        />
      </div>

      {/* Insights Row */}
      <div className="grid grid-cols-3 gap-4">
        <InsightCard
          icon={Zap}
          label="Most Liked Genre"
          value={topGenre}
          color="bg-violet-500/20 text-violet-400"
        />
        <InsightCard
          icon={Award}
          label="Top Artist"
          value={topArtist}
          color="bg-pink-500/20 text-pink-400"
        />
        <InsightCard
          icon={Music2}
          label="Most Loved Song"
          value={topSong}
          subtext={topLikedSongs[0]?.artist}
          color="bg-blue-500/20 text-blue-400"
        />
      </div>

      {/* Main Content */}
      <div className="grid grid-cols-3 gap-4">
        {/* Left Column */}
        <div className="space-y-4">
          <NowPlayingCard />
          <TopSongsList songs={topSongs} />
        </div>

        {/* Middle Column */}
        <div className="space-y-4">
          <TopUsersList users={topUsers} />
          <GenreChart genres={genreDistribution} />
        </div>

        {/* Right Column */}
        <div className="space-y-4">
          <DiscoveryBreakdown sources={discoveryBreakdown} />
          <HelpfulUsers users={helpfulUsers} />
          <RecentActivity history={recentHistory} />
        </div>
      </div>
    </div>
  );
}
