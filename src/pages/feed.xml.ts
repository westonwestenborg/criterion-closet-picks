import rss from '@astrojs/rss';
import type { APIRoute } from 'astro';
import { getPublishableGuests } from '../lib/data';

export const prerender = true;

export const GET: APIRoute = (context) => {
  const guests = getPublishableGuests()
    .filter((guest) => guest.episode_date)
    .sort((a, b) => new Date(b.episode_date).getTime() - new Date(a.episode_date).getTime())
    .slice(0, 100);

  return rss({
    title: 'Closet Picks',
    description: 'Recently aired Criterion Closet Picks episodes and their film selections.',
    site: context.site!,
    items: guests.map((guest) => ({
      title: `${guest.name}'s Criterion Closet Picks`,
      description: `${guest.name} chose ${guest.pick_count} ${guest.pick_count === 1 ? 'film' : 'films'} from the Criterion Closet.`,
      pubDate: new Date(guest.episode_date),
      link: `/guests/${guest.slug}/`,
    })),
    customData: '<language>en-us</language>',
  });
};
