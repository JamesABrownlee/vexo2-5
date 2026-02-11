import { NextResponse } from 'next/server';
import { getAllUsers } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
    try {
        const users = await getAllUsers(100);
        return NextResponse.json({ users });
    } catch (error) {
        console.error('Users API error:', error);
        return NextResponse.json({ error: 'Failed to fetch users' }, { status: 500 });
    }
}
