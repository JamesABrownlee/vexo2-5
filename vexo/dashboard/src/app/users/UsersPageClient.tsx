'use client';

import { Users as UsersIcon, Search, ChevronDown, ArrowUpDown, TrendingUp, Heart, ListMusic } from 'lucide-react';
import { useState, useMemo } from 'react';
import Link from 'next/link';

interface User {
    id: string;
    username: string;
    created_at: string;
    last_active: string;
    plays: number;
    reactions: number;
    playlists: number;
}

interface UsersPageClientProps {
    initialUsers: User[];
}

type SortField = 'plays' | 'reactions' | 'playlists' | 'last_active' | 'username';
type SortOrder = 'asc' | 'desc';

function UserCard({ user, rank }: { user: User; rank: number }) {
    const formatDate = (dateStr: string) => {
        if (!dateStr) return 'Never';
        const date = new Date(dateStr);
        const now = new Date();
        const diff = now.getTime() - date.getTime();
        const days = Math.floor(diff / (1000 * 60 * 60 * 24));

        if (days === 0) return 'Today';
        if (days === 1) return 'Yesterday';
        if (days < 7) return `${days} days ago`;
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    };

    return (
        <Link
            href={`/users/${user.id}`}
            className="bento-card flex items-center gap-4 group cursor-pointer hover:border-violet-500/30 transition-all"
        >
            {/* Rank */}
            <div className="w-8 text-center">
                <span className={`text-lg font-bold ${rank <= 3 ? 'text-violet-400' : 'text-zinc-600'}`}>
                    {rank}
                </span>
            </div>

            {/* Avatar */}
            <div className="w-12 h-12 rounded-full bg-gradient-to-br from-violet-500 to-pink-500 flex items-center justify-center text-lg font-bold shrink-0 group-hover:scale-110 transition-transform">
                {user.username?.[0]?.toUpperCase() || '?'}
            </div>

            {/* Info */}
            <div className="flex-1 min-w-0">
                <p className="text-base font-semibold text-white truncate group-hover:text-violet-400 transition-colors">
                    {user.username}
                </p>
                <p className="text-sm text-zinc-500">
                    Active {formatDate(user.last_active)}
                </p>
            </div>

            {/* Stats */}
            <div className="flex gap-6">
                <div className="text-center">
                    <p className="text-xl font-bold text-white">{user.plays}</p>
                    <p className="text-xs text-zinc-500 flex items-center gap-1">
                        <TrendingUp className="w-3 h-3" />
                        Plays
                    </p>
                </div>
                <div className="text-center">
                    <p className="text-xl font-bold text-pink-400">{user.reactions}</p>
                    <p className="text-xs text-zinc-500 flex items-center gap-1">
                        <Heart className="w-3 h-3" />
                        Reactions
                    </p>
                </div>
                <div className="text-center">
                    <p className="text-xl font-bold text-green-400">{user.playlists}</p>
                    <p className="text-xs text-zinc-500 flex items-center gap-1">
                        <ListMusic className="w-3 h-3" />
                        Playlists
                    </p>
                </div>
            </div>
        </Link>
    );
}

function SortButton({
    label,
    field,
    currentField,
    currentOrder,
    onSort
}: {
    label: string;
    field: SortField;
    currentField: SortField;
    currentOrder: SortOrder;
    onSort: (field: SortField) => void;
}) {
    const isActive = currentField === field;

    return (
        <button
            onClick={() => onSort(field)}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all flex items-center gap-1 ${isActive
                    ? 'bg-violet-500/20 text-violet-400'
                    : 'bg-white/[0.04] text-zinc-400 hover:bg-white/[0.08]'
                }`}
        >
            {label}
            {isActive && (
                <ArrowUpDown className={`w-3 h-3 ${currentOrder === 'desc' ? 'rotate-180' : ''}`} />
            )}
        </button>
    );
}

export default function UsersPageClient({ initialUsers }: UsersPageClientProps) {
    const [searchQuery, setSearchQuery] = useState('');
    const [sortField, setSortField] = useState<SortField>('plays');
    const [sortOrder, setSortOrder] = useState<SortOrder>('desc');

    const handleSort = (field: SortField) => {
        if (sortField === field) {
            setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
        } else {
            setSortField(field);
            setSortOrder('desc');
        }
    };

    const filteredAndSortedUsers = useMemo(() => {
        let result = [...initialUsers];

        // Filter by search query
        if (searchQuery) {
            const query = searchQuery.toLowerCase();
            result = result.filter(user =>
                user.username?.toLowerCase().includes(query)
            );
        }

        // Sort
        result.sort((a, b) => {
            let comparison = 0;

            switch (sortField) {
                case 'plays':
                    comparison = a.plays - b.plays;
                    break;
                case 'reactions':
                    comparison = a.reactions - b.reactions;
                    break;
                case 'playlists':
                    comparison = a.playlists - b.playlists;
                    break;
                case 'last_active':
                    comparison = new Date(a.last_active).getTime() - new Date(b.last_active).getTime();
                    break;
                case 'username':
                    comparison = (a.username || '').localeCompare(b.username || '');
                    break;
            }

            return sortOrder === 'asc' ? comparison : -comparison;
        });

        return result;
    }, [initialUsers, searchQuery, sortField, sortOrder]);

    // Calculate stats
    const totalPlays = initialUsers.reduce((sum, u) => sum + u.plays, 0);
    const totalReactions = initialUsers.reduce((sum, u) => sum + u.reactions, 0);
    const avgPlaysPerUser = initialUsers.length > 0 ? Math.round(totalPlays / initialUsers.length) : 0;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                        <UsersIcon className="w-7 h-7 text-violet-500" />
                        Users
                    </h1>
                    <p className="text-sm text-zinc-500 mt-1">
                        {initialUsers.length} users tracked · {totalPlays} total plays · {totalReactions} reactions
                    </p>
                </div>
            </div>

            {/* Quick Stats */}
            <div className="grid grid-cols-4 gap-4">
                <div className="bento-card text-center">
                    <p className="text-3xl font-bold text-white">{initialUsers.length}</p>
                    <p className="text-sm text-zinc-500">Total Users</p>
                </div>
                <div className="bento-card text-center">
                    <p className="text-3xl font-bold text-violet-400">{totalPlays}</p>
                    <p className="text-sm text-zinc-500">Total Plays</p>
                </div>
                <div className="bento-card text-center">
                    <p className="text-3xl font-bold text-pink-400">{totalReactions}</p>
                    <p className="text-sm text-zinc-500">Total Reactions</p>
                </div>
                <div className="bento-card text-center">
                    <p className="text-3xl font-bold text-green-400">{avgPlaysPerUser}</p>
                    <p className="text-sm text-zinc-500">Avg Plays/User</p>
                </div>
            </div>

            {/* Search & Sort Controls */}
            <div className="flex items-center gap-4">
                {/* Search */}
                <div className="flex items-center gap-2 px-4 py-2 rounded-xl bg-white/[0.04] border border-white/[0.08] flex-1 max-w-sm">
                    <Search className="w-4 h-4 text-zinc-400" />
                    <input
                        type="text"
                        placeholder="Search users..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="bg-transparent text-sm text-white placeholder-zinc-500 outline-none w-full"
                    />
                </div>

                {/* Sort Options */}
                <div className="flex items-center gap-2">
                    <span className="text-sm text-zinc-500">Sort by:</span>
                    <SortButton label="Plays" field="plays" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
                    <SortButton label="Reactions" field="reactions" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
                    <SortButton label="Recent" field="last_active" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
                    <SortButton label="Name" field="username" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
                </div>
            </div>

            {/* Results Info */}
            {searchQuery && (
                <p className="text-sm text-zinc-500">
                    Showing {filteredAndSortedUsers.length} of {initialUsers.length} users
                </p>
            )}

            {/* Users List */}
            <div className="space-y-3">
                {filteredAndSortedUsers.length === 0 ? (
                    <div className="bento-card text-center py-12">
                        <UsersIcon className="w-12 h-12 text-zinc-600 mx-auto mb-4" />
                        <p className="text-lg font-medium text-zinc-400">
                            {searchQuery ? 'No users match your search' : 'No users yet'}
                        </p>
                        <p className="text-sm text-zinc-600">
                            {searchQuery ? 'Try a different search term' : 'Users will appear here after they interact with the bot'}
                        </p>
                    </div>
                ) : (
                    filteredAndSortedUsers.map((user, i) => (
                        <UserCard key={user.id} user={user} rank={i + 1} />
                    ))
                )}
            </div>
        </div>
    );
}
