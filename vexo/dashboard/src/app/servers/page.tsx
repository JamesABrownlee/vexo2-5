import { Server, Music2, Users, Pause, SkipForward, Square } from 'lucide-react';
import { getGuilds } from '@/lib/db';

export const dynamic = 'force-dynamic';

interface ServerData {
    id: string;
    name: string;
    isPlaying: boolean;
    listeners: number;
}

function ServerCard({ server }: { server: ServerData }) {
    return (
        <div className="bento-card group">
            <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                    <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-violet-500/20 to-pink-500/20 flex items-center justify-center">
                        <Server className="w-6 h-6 text-violet-400" />
                    </div>
                    <div>
                        <h3 className="text-base font-semibold text-white">{server.name || 'Unknown Server'}</h3>
                        <p className="text-xs text-zinc-500">ID: {server.id}</p>
                    </div>
                </div>
                <div className={`status-dot ${server.isPlaying ? 'online' : 'offline'}`} />
            </div>

            {/* Status */}
            {server.isPlaying ? (
                <div className="p-3 rounded-xl bg-white/[0.04] mb-4">
                    <div className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-lg bg-violet-500/20 flex items-center justify-center">
                            <Music2 className="w-5 h-5 text-violet-400" />
                        </div>
                        <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-white truncate">Active Session</p>
                            <p className="text-xs text-zinc-500 truncate">Playing in voice channel</p>
                        </div>
                    </div>
                </div>
            ) : (
                <div className="p-3 rounded-xl bg-white/[0.04] mb-4 text-center">
                    <p className="text-sm text-zinc-500">Idle</p>
                </div>
            )}

            {/* Controls & Listeners */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-1 text-xs text-zinc-500">
                    <Users className="w-4 h-4" />
                    <span>{server.listeners} listeners</span>
                </div>
                <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button className="p-2 rounded-lg hover:bg-white/[0.08] transition-colors">
                        <Pause className="w-4 h-4 text-zinc-400" />
                    </button>
                    <button className="p-2 rounded-lg hover:bg-white/[0.08] transition-colors">
                        <SkipForward className="w-4 h-4 text-zinc-400" />
                    </button>
                    <button className="p-2 rounded-lg hover:bg-white/[0.08] transition-colors">
                        <Square className="w-4 h-4 text-zinc-400" />
                    </button>
                </div>
            </div>
        </div>
    );
}

export default async function ServersPage() {
    let servers: ServerData[] = [];

    try {
        servers = await getGuilds() as unknown as ServerData[];
    } catch (error) {
        console.error('Failed to fetch servers:', error);
    }

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                        <Server className="w-7 h-7 text-violet-500" />
                        Servers
                    </h1>
                    <p className="text-sm text-zinc-500 mt-1">
                        {servers.length} servers joined
                    </p>
                </div>
            </div>

            {/* Server Grid */}
            <div className="grid grid-cols-3 gap-4">
                {servers.length === 0 ? (
                    <div className="col-span-3 bento-card text-center py-12">
                        <Server className="w-12 h-12 text-zinc-600 mx-auto mb-4" />
                        <p className="text-lg font-medium text-zinc-400">No servers found</p>
                        <p className="text-sm text-zinc-600">
                            The bot hasn't joined any servers yet or data is unavailable.
                        </p>
                    </div>
                ) : (
                    servers.map((server) => (
                        <ServerCard key={server.id} server={server} />
                    ))
                )}
            </div>
        </div>
    );
}
