import { ImageResponse } from 'next/og';
import { NextRequest } from 'next/server';

export const runtime = 'edge';

const Icons = {
    List: () => (
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <line x1="8" y1="6" x2="21" y2="6" />
            <line x1="8" y1="12" x2="21" y2="12" />
            <line x1="8" y1="18" x2="21" y2="18" />
            <line x1="3" y1="6" x2="3.01" y2="6" />
            <line x1="3" y1="12" x2="3.01" y2="12" />
            <line x1="3" y1="18" x2="3.01" y2="18" />
        </svg>
    ),
    Music: () => (
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M9 18V5l12-2v13" />
            <circle cx="6" cy="18" r="3" />
            <circle cx="18" cy="16" r="3" />
        </svg>
    ),
    User: () => (
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2" />
            <circle cx="12" cy="7" r="4" />
        </svg>
    ),
};

export async function GET(req: NextRequest) {
    const { searchParams } = new URL(req.url);

    // Get queue data from encoded JSON param
    const queueDataRaw = searchParams.get('items');
    let items = [];
    try {
        if (queueDataRaw) {
            items = JSON.parse(decodeURIComponent(queueDataRaw));
        }
    } catch (e) {
        console.error("Failed to parse queue data", e);
    }

    const guildName = searchParams.get('guild') || 'Server Queue';

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
                {/* Background Glow */}
                <div
                    style={{
                        position: 'absolute',
                        top: '0',
                        left: '0',
                        right: '0',
                        bottom: '0',
                        backgroundImage: 'radial-gradient(circle at 50% 50%, rgba(124, 58, 237, 0.1) 0%, rgba(10, 10, 10, 0) 70%)',
                        display: 'flex',
                    }}
                />

                {/* Main Card */}
                <div
                    style={{
                        display: 'flex',
                        flexDirection: 'column',
                        width: '740px',
                        height: '500px',
                        backgroundColor: 'rgba(10, 10, 10, 0.8)',
                        backgroundImage: 'linear-gradient(180deg, rgba(255, 255, 255, 0.03) 0%, rgba(255, 255, 255, 0) 100%)',
                        border: '1px solid rgba(255, 255, 255, 0.08)',
                        borderRadius: '40px',
                        padding: '40px',
                        boxShadow: '0 50px 100px -30px rgba(0, 0, 0, 0.9)',
                        overflow: 'hidden',
                    }}
                >
                    {/* Header */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '16px', marginBottom: '32px' }}>
                        <div style={{ display: 'flex', color: '#8b5cf6' }}>
                            <Icons.List />
                        </div>
                        <div style={{ display: 'flex', flexDirection: 'column' }}>
                            <span style={{ fontSize: '13px', fontWeight: 800, color: 'rgba(255,255,255,0.4)', letterSpacing: '0.2em', textTransform: 'uppercase' }}>
                                Up Next
                            </span>
                            <span style={{ fontSize: '24px', fontWeight: 800, color: 'white' }}>
                                {guildName}
                            </span>
                        </div>
                    </div>

                    {/* Queue List */}
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                        {items.length === 0 ? (
                            <div style={{ display: 'flex', flex: 1, alignItems: 'center', justifyContent: 'center', padding: '40px', color: 'rgba(255,255,255,0.2)', fontSize: '18px', fontWeight: 500 }}>
                                Queue is empty. Autoplay will pick the next song!
                            </div>
                        ) : (
                            items.slice(0, 5).map((item: any, index: number) => (
                                <div
                                    key={index}
                                    style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        padding: '16px 24px',
                                        backgroundColor: 'rgba(255, 255, 255, 0.03)',
                                        border: '1px solid rgba(255, 255, 255, 0.05)',
                                        borderRadius: '20px',
                                        gap: '20px',
                                    }}
                                >
                                    <div style={{
                                        display: 'flex',
                                        width: '40px',
                                        height: '40px',
                                        borderRadius: '12px',
                                        backgroundColor: 'rgba(139, 92, 246, 0.1)',
                                        color: '#8b5cf6',
                                        alignItems: 'center',
                                        justifyContent: 'center',
                                        fontSize: '16px',
                                        fontWeight: 800
                                    }}>
                                        {index + 1}
                                    </div>

                                    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden' }}>
                                        <span style={{
                                            fontSize: '17px',
                                            fontWeight: 700,
                                            color: 'white',
                                            whiteSpace: 'nowrap',
                                            overflow: 'hidden',
                                            textOverflow: 'ellipsis'
                                        }}>
                                            {item.title}
                                        </span>
                                        <span style={{
                                            fontSize: '14px',
                                            fontWeight: 500,
                                            color: 'rgba(255,255,255,0.4)',
                                            textTransform: 'uppercase',
                                            letterSpacing: '0.05em'
                                        }}>
                                            {item.artist}
                                        </span>
                                    </div>

                                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px', backgroundColor: 'rgba(255, 255, 255, 0.05)', padding: '6px 12px', borderRadius: '10px' }}>
                                        <div style={{ display: 'flex', color: 'rgba(255,255,255,0.3)' }}>
                                            <Icons.User />
                                        </div>
                                        <span style={{ fontSize: '12px', fontWeight: 600, color: 'rgba(255,255,255,0.6)' }}>
                                            {item.requester || 'Discovery'}
                                        </span>
                                    </div>
                                </div>
                            ))
                        )}

                        {items.length > 5 && (
                            <div style={{ display: 'flex', justifyContent: 'center', marginTop: '8px' }}>
                                <span style={{ fontSize: '14px', color: 'rgba(255,255,255,0.3)', fontWeight: 600 }}>
                                    + {items.length - 5} more tracks in queue
                                </span>
                            </div>
                        )}
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
