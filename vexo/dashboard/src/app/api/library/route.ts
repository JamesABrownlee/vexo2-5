import { NextResponse } from 'next/server';
import { getLibrary } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
    try {
        const { searchParams } = new URL(request.url);
        const limit = parseInt(searchParams.get('limit') || '200', 10);

        const library = await getLibrary(limit);

        return NextResponse.json({ library });
    } catch (error) {
        console.error('Library API error:', error);
        return NextResponse.json({ error: 'Failed to fetch library' }, { status: 500 });
    }
}
