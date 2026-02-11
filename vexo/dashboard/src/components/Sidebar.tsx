'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
    LayoutDashboard,
    Library,
    Clock,
    Users,
    Server,
    FileText,
    Settings,
    Music2,
    Cog,
} from 'lucide-react';

const navItems = [
    { href: '/', label: 'Dashboard', icon: LayoutDashboard },
    { href: '/library', label: 'Library', icon: Library },
    { href: '/history', label: 'History', icon: Clock },
    { href: '/users', label: 'Users', icon: Users },
    { href: '/servers', label: 'Servers', icon: Server },
    { divider: true },
    { href: '/logs', label: 'Logs', icon: FileText },
    { href: '/settings', label: 'Settings', icon: Settings },
    { href: '/services', label: 'Services', icon: Cog },
];

export function Sidebar() {
    const pathname = usePathname();

    return (
        <aside className="w-64 bg-[#141415] border-r border-white/[0.08] flex flex-col">
            {/* Logo */}
            <div className="p-6 border-b border-white/[0.08]">
                <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-500 to-pink-500 flex items-center justify-center">
                        <Music2 className="w-5 h-5 text-white" />
                    </div>
                    <span className="text-xl font-bold gradient-text">Vexo</span>
                </div>
            </div>

            {/* Navigation */}
            <nav className="flex-1 p-4 space-y-1">
                {navItems.map((item, index) => {
                    if ('divider' in item) {
                        return <div key={index} className="h-px bg-white/[0.08] my-4" />;
                    }

                    const Icon = item.icon;
                    const isActive = pathname === item.href;

                    return (
                        <Link
                            key={item.href}
                            href={item.href}
                            className={`flex items-center gap-3 px-4 py-2.5 rounded-xl text-sm font-medium transition-all ${isActive
                                    ? 'bg-white/[0.08] text-white'
                                    : 'text-zinc-400 hover:text-white hover:bg-white/[0.04]'
                                }`}
                        >
                            <Icon className="w-5 h-5" />
                            <span>{item.label}</span>
                        </Link>
                    );
                })}
            </nav>

            {/* Status */}
            <div className="p-4 border-t border-white/[0.08]">
                <div className="flex items-center gap-3">
                    <div className="status-dot online" />
                    <div className="flex flex-col">
                        <span className="text-sm font-medium">Connected</span>
                        <span className="text-xs text-zinc-500">--ms latency</span>
                    </div>
                </div>
            </div>
        </aside>
    );
}
