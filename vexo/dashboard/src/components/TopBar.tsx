'use client';

import { Bell, Search, Moon } from 'lucide-react';
import { useState } from 'react';

export function TopBar() {
    const [searchOpen, setSearchOpen] = useState(false);

    return (
        <header className="h-16 bg-[#141415] border-b border-white/[0.08] flex items-center justify-between px-6">
            {/* Server Pills */}
            <nav className="flex items-center gap-2">
                <button className="px-4 py-2 rounded-full bg-white/[0.08] text-sm font-medium text-white flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-violet-500" />
                    Global
                </button>
                {/* Server pills will be dynamically populated */}
            </nav>

            {/* Right Section */}
            <div className="flex items-center gap-4">
                {/* Search */}
                <div className="relative">
                    <div className="flex items-center gap-2 px-4 py-2 rounded-xl bg-white/[0.04] border border-white/[0.08] hover:border-white/[0.12] transition-all cursor-pointer"
                        onClick={() => setSearchOpen(true)}>
                        <Search className="w-4 h-4 text-zinc-400" />
                        <span className="text-sm text-zinc-500">Search...</span>
                        <kbd className="ml-8 px-2 py-0.5 text-xs rounded bg-white/[0.08] text-zinc-400 font-mono">
                            ⌘K
                        </kbd>
                    </div>
                </div>

                {/* Theme Toggle */}
                <button className="p-2 rounded-xl hover:bg-white/[0.08] transition-colors">
                    <Moon className="w-5 h-5 text-zinc-400" />
                </button>

                {/* Notifications */}
                <button className="relative p-2 rounded-xl hover:bg-white/[0.08] transition-colors">
                    <Bell className="w-5 h-5 text-zinc-400" />
                    <span className="absolute top-1 right-1 w-2 h-2 rounded-full bg-violet-500" />
                </button>
            </div>

            {/* Search Modal */}
            {searchOpen && (
                <div className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm flex items-start justify-center pt-32"
                    onClick={() => setSearchOpen(false)}>
                    <div className="w-full max-w-xl bg-[#141415] rounded-2xl border border-white/[0.08] shadow-2xl"
                        onClick={e => e.stopPropagation()}>
                        <div className="flex items-center gap-3 p-4 border-b border-white/[0.08]">
                            <Search className="w-5 h-5 text-zinc-400" />
                            <input
                                type="text"
                                placeholder="Search songs, users, servers..."
                                className="flex-1 bg-transparent text-white placeholder-zinc-500 outline-none"
                                autoFocus
                            />
                            <kbd className="px-2 py-1 text-xs rounded bg-white/[0.08] text-zinc-400 font-mono">
                                ESC
                            </kbd>
                        </div>
                        <div className="p-4">
                            <p className="text-sm text-zinc-500">Start typing to search...</p>
                        </div>
                    </div>
                </div>
            )}
        </header>
    );
}
