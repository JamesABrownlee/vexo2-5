import { getAllUsers } from '@/lib/db';
import UsersPageClient from './UsersPageClient';

export const dynamic = 'force-dynamic';

export default async function UsersPage() {
    let users: Array<{
        id: string;
        username: string;
        created_at: string;
        last_active: string;
        plays: number;
        reactions: number;
        playlists: number;
    }> = [];

    try {
        users = await getAllUsers(100) as typeof users;
    } catch (error) {
        console.error('Failed to fetch users:', error);
    }

    return <UsersPageClient initialUsers={users} />;
}
