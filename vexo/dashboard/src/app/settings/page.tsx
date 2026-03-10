'use client';

import { Settings as SettingsIcon, Save, Globe, Volume2, Sparkles, Shield, Brain } from 'lucide-react';
import { useState, useEffect } from 'react';

interface SettingToggle {
    id: string;
    label: string;
    description: string;
    enabled: boolean;
}

export default function SettingsPage() {
    const [settings, setSettings] = useState<SettingToggle[]>([
        {
            id: 'discovery_enabled',
            label: 'Discovery Mode',
            description: 'Automatically play related songs when the queue is empty',
            enabled: true,
        },
        {
            id: 'prefer_similar',
            label: 'Prefer Similar Songs',
            description: 'Discovery prioritizes songs similar to what the group likes',
            enabled: true,
        },
        {
            id: 'democratic_mode',
            label: 'Democratic Mode',
            description: 'Take turns picking songs based on who\'s listening',
            enabled: true,
        },
        {
            id: 'reaction_learning',
            label: 'Reaction Learning',
            description: 'Learn preferences from emoji reactions (❤️ likes, 👎 dislikes)',
            enabled: true,
        },
        {
            id: 'announce_songs',
            label: 'Announce Songs',
            description: 'Send a message when a new song starts playing',
            enabled: false,
        },
    ]);

    const [aiSettings, setAiSettings] = useState<SettingToggle[]>([
        {
            id: 'ai_discovery_enabled',
            label: 'AI-Powered Discovery',
            description: 'Use Local AI to suggest songs based on user preferences',
            enabled: false,
        },
        {
            id: 'ai_discovery_on_join',
            label: 'AI Recommendations on Join',
            description: 'Queue personalized AI suggestions when users join the voice channel',
            enabled: false,
        },
    ]);

    const [aiStatus, setAiStatus] = useState<any>(null);

    useEffect(() => {
        // Fetch AI provider status from backend
        fetch('/api/services/ai/status')
            .then(r => r.json())
            .then(setAiStatus)
            .catch(() => setAiStatus(null));
    }, []);

    const [defaultVolume, setDefaultVolume] = useState(50);
    const [discoveryChance, setDiscoveryChance] = useState(70);

    const toggleSetting = (id: string) => {
        setSettings(prev =>
            prev.map(s => (s.id === id ? { ...s, enabled: !s.enabled } : s))
        );
    };

    const toggleAiSetting = (id: string) => {
        setAiSettings(prev =>
            prev.map(s => (s.id === id ? { ...s, enabled: !s.enabled } : s))
        );
    };

    return (
        <div className="space-y-6 max-w-3xl">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                        <SettingsIcon className="w-7 h-7 text-violet-500" />
                        Settings
                    </h1>
                    <p className="text-sm text-zinc-500 mt-1">
                        Configure global bot settings
                    </p>
                </div>
                <button className="flex items-center gap-2 px-4 py-2 rounded-xl bg-violet-500 text-white font-medium hover:bg-violet-600 transition-colors">
                    <Save className="w-4 h-4" />
                    Save Changes
                </button>
            </div>

            {/* General Settings */}
            <div className="bento-card">
                <h2 className="text-lg font-semibold text-white flex items-center gap-2 mb-6">
                    <Globe className="w-5 h-5 text-violet-500" />
                    General
                </h2>
                <div className="space-y-4">
                    {settings.map((setting) => (
                        <div key={setting.id} className="flex items-center justify-between p-3 rounded-xl hover:bg-white/[0.04] transition-colors">
                            <div>
                                <p className="text-sm font-medium text-white">{setting.label}</p>
                                <p className="text-xs text-zinc-500 mt-0.5">{setting.description}</p>
                            </div>
                            <button
                                onClick={() => toggleSetting(setting.id)}
                                className={`relative w-12 h-6 rounded-full transition-colors ${setting.enabled ? 'bg-violet-500' : 'bg-zinc-700'
                                    }`}
                            >
                                <span
                                    className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-all ${setting.enabled ? 'left-7' : 'left-1'
                                        }`}
                                />
                            </button>
                        </div>
                    ))}
                </div>
            </div>

            {/* AI Discovery Settings */}
            <div className="bento-card bg-gradient-to-br from-violet-500/5 to-pink-500/5 border-violet-500/20">
                <h2 className="text-lg font-semibold text-white flex items-center gap-2 mb-6">
                    <Brain className="w-5 h-5 text-violet-400" />
                    AI Discovery
                    <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-violet-500/20 text-violet-400">Beta</span>
                </h2>
                <div className="space-y-4">
                    {aiSettings.map((setting) => (
                        <div key={setting.id} className="flex items-center justify-between p-3 rounded-xl hover:bg-white/[0.04] transition-colors">
                            <div>
                                <p className="text-sm font-medium text-white">{setting.label}</p>
                                <p className="text-xs text-zinc-500 mt-0.5">{setting.description}</p>
                            </div>
                            <button
                                onClick={() => toggleAiSetting(setting.id)}
                                className={`relative w-12 h-6 rounded-full transition-colors ${setting.enabled ? 'bg-violet-500' : 'bg-zinc-700'
                                    }`}
                            >
                                <span
                                    className={`absolute top-1 w-4 h-4 rounded-full bg-white transition-all ${setting.enabled ? 'left-7' : 'left-1'
                                        }`}
                                />
                            </button>
                        </div>
                    ))}
                </div>
                <div className="mt-4 p-3 rounded-xl bg-violet-500/10 border border-violet-500/20">
                    <p className="text-xs text-violet-300">
                        <strong>New:</strong> Use <code className="px-1 py-0.5 rounded bg-violet-500/20">/play ai &lt;song&gt;</code> to queue a seed song with AI-generated follow-ups.
                    </p>

                    <div className="mt-3">
                        <label className="text-xs text-zinc-400">Local AI Provider</label>
                        <div className="mt-2 flex items-center gap-2">
                            <button className={`px-3 py-1 rounded ${aiStatus?.providers?.ollama?.available ? 'bg-violet-500 text-white' : 'bg-zinc-700 text-zinc-400'}`} disabled={!aiStatus?.providers?.ollama?.available}>
                                Ollama {aiStatus?.providers?.ollama?.available ? '(Available)' : '(Unavailable)'}
                            </button>
                            <button className={`px-3 py-1 rounded ${aiStatus?.providers?.llamacpp?.available ? 'bg-violet-500 text-white' : 'bg-zinc-700 text-zinc-400'}`} disabled={!aiStatus?.providers?.llamacpp?.available}>
                                llama.cpp {aiStatus?.providers?.llamacpp?.available ? '(Available)' : '(Unavailable)'}
                            </button>
                        </div>

                        {aiStatus && !aiStatus.ai_available && (
                            <p className="text-xs text-red-400 mt-2">AI is not available. Neither Ollama nor llama.cpp responded.</p>
                        )}
                        {aiStatus && aiStatus.message && (
                            <p className="text-xs text-zinc-300 mt-2">{aiStatus.message}</p>
                        )}
                    </div>
                </div>
            </div>

            {/* Audio Settings */}
            <div className="bento-card">
                <h2 className="text-lg font-semibold text-white flex items-center gap-2 mb-6">
                    <Volume2 className="w-5 h-5 text-violet-500" />
                    Audio
                </h2>
                <div className="space-y-6">
                    <div>
                        <div className="flex items-center justify-between mb-2">
                            <label className="text-sm font-medium text-white">Default Volume</label>
                            <span className="text-sm text-zinc-500">{defaultVolume}%</span>
                        </div>
                        <input
                            type="range"
                            min="0"
                            max="100"
                            value={defaultVolume}
                            onChange={(e) => setDefaultVolume(parseInt(e.target.value))}
                            className="w-full h-2 bg-zinc-700 rounded-full appearance-none cursor-pointer
                [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:w-4 [&::-webkit-slider-thumb]:h-4 
                [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:bg-violet-500"
                        />
                    </div>
                </div>
            </div>

            {/* Discovery Settings */}
            <div className="bento-card">
                <h2 className="text-lg font-semibold text-white flex items-center gap-2 mb-6">
                    <Sparkles className="w-5 h-5 text-violet-500" />
                    Discovery
                </h2>
                <div className="space-y-6">
                    <div>
                        <div className="flex items-center justify-between mb-2">
                            <label className="text-sm font-medium text-white">Similar Song Chance</label>
                            <span className="text-sm text-zinc-500">{discoveryChance}%</span>
                        </div>
                        <p className="text-xs text-zinc-500 mb-3">
                            How often to pick similar songs vs. same artist or wildcard
                        </p>
                        <input
                            type="range"
                            min="0"
                            max="100"
                            value={discoveryChance}
                            onChange={(e) => setDiscoveryChance(parseInt(e.target.value))}
                            className="w-full h-2 bg-zinc-700 rounded-full appearance-none cursor-pointer
                [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:w-4 [&::-webkit-slider-thumb]:h-4 
                [&::-webkit-slider-thumb]:rounded-full [&::-webkit-slider-thumb]:bg-violet-500"
                        />
                    </div>
                </div>
            </div>

            {/* Privacy */}
            <div className="bento-card bg-red-500/5 border-red-500/20">
                <h2 className="text-lg font-semibold text-white flex items-center gap-2 mb-4">
                    <Shield className="w-5 h-5 text-red-500" />
                    Privacy & Data
                </h2>
                <p className="text-sm text-zinc-400 mb-4">
                    Data management options. These actions are irreversible.
                </p>
                <div className="flex gap-3">
                    <button className="px-4 py-2 rounded-xl bg-red-500/20 text-red-500 text-sm font-medium hover:bg-red-500/30 transition-colors">
                        Clear All History
                    </button>
                    <button className="px-4 py-2 rounded-xl bg-red-500/20 text-red-500 text-sm font-medium hover:bg-red-500/30 transition-colors">
                        Reset Preferences
                    </button>
                </div>
            </div>
        </div>
    );
}
