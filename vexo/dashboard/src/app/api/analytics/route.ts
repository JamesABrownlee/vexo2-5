import { NextResponse } from 'next/server';
import { getTotalStats, getDiscoveryBreakdown, getGenreDistribution, getTopSongs, getTopUsers, getTopLikedArtists, getTopLikedGenres } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
    try {
        const stats = await getTotalStats();
        const discoveryBreakdown = await getDiscoveryBreakdown();
        const genreDistribution = await getGenreDistribution(10);
        const topSongs = await getTopSongs(10);
        const topUsers = await getTopUsers(10);
        const topArtists = await getTopLikedArtists(10);
        const topGenres = await getTopLikedGenres(10);

        return NextResponse.json({
            stats,
            discoveryBreakdown,
            genreDistribution,
            topSongs,
            topUsers,
            topArtists,
            topGenres,
        });
    } catch (error) {
        console.error('Analytics error:', error);
        return NextResponse.json(
            { error: 'Failed to fetch analytics' },
            { status: 500 }
        );
    }
}
