import { NextResponse } from 'next/server';
import { getUserDetail } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(
    request: Request,
    { params }: { params: Promise<{ id: string }> }
) {
    try {
        const { id } = await params;

        const userData = await getUserDetail(id);

        if (!userData) {
            return NextResponse.json({ error: 'User not found' }, { status: 404 });
        }

        return NextResponse.json(userData);
    } catch (error) {
        console.error('User detail API error:', error);
        return NextResponse.json({ error: 'Failed to fetch user' }, { status: 500 });
    }
}
