import { getRecentHistory } from '@/lib/db';
import HistoryPageClient from './HistoryPageClient';

export const dynamic = 'force-dynamic';

interface HistoryItem {
    played_at: string;
    title: string;
    artist_name: string;
    album: string | null;
    release_year: number | null;
    duration_seconds: number | null;
    yt_id: string | null;
    genre: string | null;
    discovery_source: string | null;
    discovery_reason: string | null;
    completed: number;
    skip_reason: string | null;
    requested_by: string | null;
    liked_by: string | null;
    disliked_by: string | null;
}

export default async function HistoryPage() {
    let history: HistoryItem[] = [];

    try {
        history = (await getRecentHistory(500) as unknown) as HistoryItem[];
    } catch (error) {
        console.error('Failed to fetch history:', error);
    }

    return <HistoryPageClient initialHistory={history} />;
}

