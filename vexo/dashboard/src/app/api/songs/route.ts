import { NextResponse } from 'next/server';
import { getRecentHistory } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
    try {
        const { searchParams } = new URL(request.url);
        const limit = parseInt(searchParams.get('limit') || '100', 10);
        const guildId = searchParams.get('guild_id');

        const history = await getRecentHistory(
            limit,
            guildId ? parseInt(guildId, 10) : undefined
        );

        return NextResponse.json({ history });
    } catch (error) {
        console.error('Songs API error:', error);
        return NextResponse.json({ error: 'Failed to fetch songs' }, { status: 500 });
    }
}
