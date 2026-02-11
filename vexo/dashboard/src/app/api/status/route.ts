import { NextResponse } from 'next/server';

export async function GET() {
    return NextResponse.json({
        status: 'online',
        timestamp: new Date().toISOString(),
        version: '1.0.0'
    });
}
