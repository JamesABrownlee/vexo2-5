import { ImageResponse } from 'next/og';
import { NextRequest } from 'next/server';

export const runtime = 'edge';

// SVG Icons as React Components/Elements
const Icons = {
    Music: () => (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M9 18V5l12-2v13" />
            <circle cx="6" cy="18" r="3" />
            <circle cx="18" cy="16" r="3" />
        </svg>
    ),
    User: () => (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2" />
            <circle cx="12" cy="7" r="4" />
        </svg>
    ),
    Clock: () => (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="10" />
            <polyline points="12 6 12 12 16 14" />
        </svg>
    ),
    Calendar: () => (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
            <line x1="16" y1="2" x2="16" y2="6" />
            <line x1="8" y1="2" x2="8" y2="6" />
            <line x1="3" y1="10" x2="21" y2="10" />
        </svg>
    ),
    Sparkles: () => (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z" />
            <path d="M5 3v4" />
            <path d="M19 17v4" />
            <path d="M3 5h4" />
            <path d="M17 19h4" />
        </svg>
    )
};

export async function GET(req: NextRequest) {
    const { searchParams } = new URL(req.url);

    // Get track info from query params
    const title = searchParams.get('title') || 'Unknown Title';
    const artist = searchParams.get('artist') || 'Unknown Artist';
    const thumbnail = searchParams.get('thumbnail') || '';
    const genre = searchParams.get('genre') || 'Music';
    const year = searchParams.get('year') || '2024';
    const progress = Math.max(0, Math.min(100, parseInt(searchParams.get('progress') || '0')));
    const duration = searchParams.get('duration') || '0:00';
    const current = searchParams.get('current') || '0:00';

    // User info logic: searchParams prioritize forUser as the person currently listening
    const requestedBy = searchParams.get('requestedBy');
    const forUser = searchParams.get('forUser');

    // Clean up emojis from discovery reason if any
    let discoveryReason = searchParams.get('discoveryReason') || '';
    discoveryReason = discoveryReason.replace(/[\u{1F300}-\u{1F9FF}]/gu, '').replace(/[\u{2600}-\u{26FF}]/gu, '').trim();

    // Actual username: prioritize forUser (the listener), then requestedBy
    const username = forUser || requestedBy || 'Listener';

    return new ImageResponse(
        (
            <div
                style={{
                    height: '100%',
                    width: '100%',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    backgroundColor: '#0a0a0a',
                    color: 'white',
                    fontFamily: 'Inter, system-ui, sans-serif',
                }}
            >
                {/* Background Artwork Blur */}
                <div
                    style={{
                        position: 'absolute',
                        top: '-15%',
                        left: '-15%',
                        right: '-15%',
                        bottom: '-15%',
                        backgroundImage: thumbnail ? `url(${thumbnail})` : 'none',
                        backgroundSize: 'cover',
                        backgroundPosition: 'center',
                        filter: 'blur(100px) brightness(0.2)',
                        display: 'flex',
                    }}
                />

                {/* Main Content Card */}
                <div
                    style={{
                        display: 'flex',
                        flexDirection: 'column',
                        position: 'relative',
                        width: '740px',
                        height: '500px',
                        backgroundColor: 'rgba(10, 10, 10, 0.6)',
                        backgroundImage: 'linear-gradient(180deg, rgba(255, 255, 255, 0.03) 0%, rgba(255, 255, 255, 0) 100%)',
                        border: '1px solid rgba(255, 255, 255, 0.08)',
                        borderRadius: '40px',
                        padding: '48px',
                        boxShadow: '0 50px 100px -30px rgba(0, 0, 0, 0.9)',
                        overflow: 'hidden',
                    }}
                >
                    {/* Top Section: Title and Artwork */}
                    <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', marginBottom: '40px', alignItems: 'flex-start' }}>
                        <div style={{ display: 'flex', flexDirection: 'column', flex: 1, marginRight: '40px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '24px' }}>
                                <div style={{ display: 'flex', color: '#8b5cf6', opacity: 0.8 }}>
                                    <Icons.Music />
                                </div>
                                <span style={{ fontSize: '15px', fontWeight: 700, color: 'rgba(255,255,255,0.4)', letterSpacing: '0.2em', textTransform: 'uppercase', display: 'flex' }}>
                                    Now Playing
                                </span>
                            </div>
                            <span style={{
                                fontSize: '32px',
                                fontWeight: 800,
                                color: 'white',
                                lineHeight: 1.25,
                                marginBottom: '4px',
                                display: 'flex',
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis'
                            }}>
                                {title}
                            </span>
                            <span style={{
                                fontSize: '20px',
                                fontWeight: 500,
                                color: 'rgba(255,255,255,0.5)',
                                display: 'flex',
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                textTransform: 'uppercase',
                                letterSpacing: '0.02em'
                            }}>
                                {artist}
                            </span>
                        </div>
                        <div style={{
                            display: 'flex',
                            width: '160px',
                            height: '160px',
                            borderRadius: '32px',
                            overflow: 'hidden',
                            border: '1px solid rgba(255, 255, 255, 0.1)',
                            boxShadow: '0 30px 60px rgba(0,0,0,0.7)',
                            flexShrink: 0,
                            backgroundColor: '#151515'
                        }}>
                            <img
                                src={thumbnail || 'https://via.placeholder.com/256'}
                                style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                            />
                        </div>
                    </div>

                    {/* Middle Section: Meta Info */}
                    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, justifyContent: 'center' }}>
                        <div style={{ display: 'flex', width: '100%', gap: '48px', marginBottom: '32px' }}>
                            <div style={{ display: 'flex', flexDirection: 'column' }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'rgba(255,255,255,0.3)', fontSize: '12px', fontWeight: 700, marginBottom: '10px', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
                                    <Icons.Calendar />
                                    <span style={{ display: 'flex' }}>Details</span>
                                </div>
                                <span style={{ fontSize: '17px', fontWeight: 600, color: 'rgba(255,255,255,0.9)', display: 'flex' }}>
                                    {year} • {genre}
                                </span>
                            </div>

                            <div style={{ display: 'flex', flexDirection: 'column' }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'rgba(255,255,255,0.3)', fontSize: '12px', fontWeight: 700, marginBottom: '10px', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
                                    <Icons.Clock />
                                    <span style={{ display: 'flex' }}>Duration</span>
                                </div>
                                <span style={{ fontSize: '17px', fontWeight: 600, color: 'rgba(255,255,255,0.9)', display: 'flex' }}>
                                    {duration}
                                </span>
                            </div>

                            <div style={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', color: 'rgba(255,255,255,0.3)', fontSize: '12px', fontWeight: 700, marginBottom: '10px', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
                                    <Icons.User />
                                    <span style={{ display: 'flex' }}>{discoveryReason ? 'Playing For' : 'Requested By'}</span>
                                </div>
                                <span style={{ fontSize: '17px', fontWeight: 600, color: '#a78bfa', display: 'flex' }}>
                                    {username}
                                </span>
                            </div>
                        </div>

                        {/* Enhanced Discovery Bar */}
                        {discoveryReason && (
                            <div style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: '12px',
                                padding: '14px 20px',
                                backgroundColor: 'rgba(251, 191, 36, 0.05)',
                                borderRadius: '20px',
                                border: '1px solid rgba(251, 191, 36, 0.1)',
                                alignSelf: 'flex-start'
                            }}>
                                <div style={{ display: 'flex', color: '#fbbf24', opacity: 0.8 }}>
                                    <Icons.Sparkles />
                                </div>
                                <span style={{ fontSize: '14px', color: 'rgba(255,255,255,0.7)', fontWeight: 500, display: 'flex' }}>
                                    {discoveryReason}
                                </span>
                            </div>
                        )}
                    </div>

                    {/* Footer: Progress Player */}
                    <div style={{ display: 'flex', flexDirection: 'column', width: '100%', marginTop: 'auto' }}>
                        <div style={{
                            width: '100%',
                            height: '4px',
                            backgroundColor: 'rgba(255, 255, 255, 0.08)',
                            borderRadius: '2px',
                            marginBottom: '16px',
                            display: 'flex',
                            position: 'relative',
                            overflow: 'hidden',
                        }}>
                            <div style={{
                                width: `${progress}%`,
                                height: '100%',
                                backgroundImage: 'linear-gradient(90deg, #7c3aed 0%, #d946ef 100%)',
                                borderRadius: '2px',
                            }} />
                        </div>
                        <div style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            fontSize: '13px',
                            color: 'rgba(255,255,255,0.35)',
                            fontWeight: 600,
                            fontVariantNumeric: 'tabular-nums'
                        }}>
                            <span style={{ display: 'flex' }}>{current}</span>
                            <span style={{ display: 'flex' }}>{duration}</span>
                        </div>
                    </div>
                </div>
            </div>
        ),
        {
            width: 800,
            height: 560,
        }
    );
}
