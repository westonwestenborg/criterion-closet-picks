type HistoryMode = 'push' | 'replace';

type BrowseState = {
  filters: Record<string, string>;
  query: string;
  sort: string;
  dir: 'asc' | 'desc';
};

const initializedGrids = new WeakSet<Element>();

function getControls(grid: HTMLElement) {
  return grid.closest('main')?.querySelector<HTMLElement>('.browse-controls') || document.querySelector<HTMLElement>('.browse-controls');
}

function normalize(value: string | null | undefined): string {
  return (value || '').trim().toLowerCase();
}

function getItemLabel(count: number, singular: string, plural: string): string {
  return count === 1 ? singular : plural;
}

function setUrl(state: BrowseState, defaults: BrowseState, mode: HistoryMode) {
  const url = new URL(window.location.href);

  if (state.query) {
    url.searchParams.set('q', state.query);
  } else {
    url.searchParams.delete('q');
  }

  Object.entries(state.filters).forEach(([key, value]) => {
    if (value && value !== 'all') {
      url.searchParams.set(key, value);
    } else {
      url.searchParams.delete(key);
    }
  });

  if (state.sort && state.sort !== defaults.sort) {
    url.searchParams.set('sort', state.sort);
  } else {
    url.searchParams.delete('sort');
  }

  if (state.dir && state.dir !== defaults.dir) {
    url.searchParams.set('dir', state.dir);
  } else {
    url.searchParams.delete('dir');
  }

  window.history[mode === 'push' ? 'pushState' : 'replaceState']({}, '', url);
}

function readStateFromUrl(controls: HTMLElement): BrowseState {
  const params = new URLSearchParams(window.location.search);
  const filters: Record<string, string> = {};

  controls.querySelectorAll<HTMLElement>('[data-filter-key]').forEach((group) => {
    const key = group.dataset.filterKey;
    if (!key) return;
    filters[key] = normalize(params.get(key)) || 'all';
  });

  const defaultSort = controls.dataset.defaultSort || '';
  const defaultDir = controls.dataset.defaultDir === 'desc' ? 'desc' : 'asc';
  const dir = params.get('dir') === 'desc' ? 'desc' : 'asc';

  return {
    filters,
    query: params.get('q') || '',
    sort: params.get('sort') || defaultSort,
    dir,
  };
}

function updateFilterButtons(controls: HTMLElement, state: BrowseState) {
  controls.querySelectorAll<HTMLElement>('[data-filter-key]').forEach((group) => {
    const key = group.dataset.filterKey;
    if (!key) return;

    const value = state.filters[key] || 'all';
    let matched = false;

    group.querySelectorAll<HTMLButtonElement>('[data-filter-value]').forEach((button) => {
      const isActive = normalize(button.dataset.filterValue) === value;
      button.classList.toggle('active', isActive);
      button.setAttribute('aria-pressed', String(isActive));
      if (isActive) matched = true;
    });

    if (!matched) {
      const allButton = group.querySelector<HTMLButtonElement>('[data-filter-value="all"]');
      if (allButton) {
        state.filters[key] = 'all';
        allButton.classList.add('active');
        allButton.setAttribute('aria-pressed', 'true');
      }
    }
  });
}

function updateSortButtons(controls: HTMLElement, state: BrowseState) {
  const buttons = Array.from(controls.querySelectorAll<HTMLButtonElement>('[data-sort]'));
  if (!buttons.length) return;

  buttons.forEach((button) => {
    const isActive = button.dataset.sort === state.sort;
    button.classList.toggle('active', isActive);
    button.setAttribute('aria-pressed', String(isActive));

    const oldArrow = button.querySelector('.sort-arrow');
    if (oldArrow) oldArrow.remove();

    if (isActive) {
      button.dataset.dir = state.dir;
      const arrow = document.createElement('span');
      arrow.className = 'sort-arrow';
      arrow.setAttribute('aria-hidden', 'true');
      arrow.textContent = state.dir === 'asc' ? '▲' : '▼';
      button.append(' ', arrow);
      button.setAttribute('aria-label', `${button.textContent?.replace(/[▲▼]/g, '').trim()} ${state.dir === 'asc' ? 'ascending' : 'descending'}`);
    } else {
      button.removeAttribute('aria-label');
    }
  });
}

function getItems(grid: HTMLElement): HTMLElement[] {
  return Array.from(grid.querySelectorAll<HTMLElement>('[data-browse-item]'));
}

function getDividers(grid: HTMLElement): HTMLElement[] {
  return Array.from(grid.querySelectorAll<HTMLElement>('[data-browse-divider]'));
}

// Grouping (letter dividers + jump rail) is only shown at the default sort:
// guests are always default (no sort controls), films only at title-asc.
function isGrouping(state: BrowseState, defaults: BrowseState): boolean {
  return state.sort === defaults.sort && state.dir === defaults.dir;
}

function sortItems(items: HTMLElement[], state: BrowseState): HTMLElement[] {
  const sorted = items.slice();
  if (!state.sort) return sorted; // no sort key (e.g. guests): keep server order

  const multiplier = state.dir === 'asc' ? 1 : -1;
  sorted.sort((a, b) => {
    if (state.sort === 'title') {
      return multiplier * (a.dataset.title || '').localeCompare(b.dataset.title || '');
    }
    const aValue = parseInt(a.dataset[state.sort] || '0', 10) || 0;
    const bValue = parseInt(b.dataset[state.sort] || '0', 10) || 0;
    return multiplier * (aValue - bValue);
  });
  return sorted;
}

// Reorders items in the DOM and manages divider + jump-rail visibility. When
// grouping, dividers are interleaved before each letter's items; otherwise all
// dividers and the rail are hidden and items are appended in flat sorted order.
function layoutGrid(grid: HTMLElement, controls: HTMLElement, state: BrowseState, defaults: BrowseState) {
  const items = getItems(grid);
  const dividers = getDividers(grid);
  const rail = controls.querySelector<HTMLElement>('[data-letter-rail]');
  const grouping = isGrouping(state, defaults) && dividers.length > 0;
  const sorted = sortItems(items, state);

  const fragment = document.createDocumentFragment();

  if (grouping) {
    const dividerByLetter = new Map<string, HTMLElement>();
    dividers.forEach((divider) => {
      dividerByLetter.set(divider.dataset.letter || '', divider);
    });

    const emitted = new Set<string>();
    sorted.forEach((item) => {
      const letter = item.dataset.letter || '';
      if (!emitted.has(letter)) {
        emitted.add(letter);
        const divider = dividerByLetter.get(letter);
        if (divider) {
          divider.hidden = false;
          fragment.appendChild(divider);
        }
      }
      fragment.appendChild(item);
    });
  } else {
    dividers.forEach((divider) => {
      divider.hidden = true;
    });
    sorted.forEach((item) => fragment.appendChild(item));
  }

  grid.appendChild(fragment);
  if (rail) rail.hidden = !grouping;

  return grouping;
}

function hasActiveState(state: BrowseState, defaults: BrowseState): boolean {
  const hasFilter = Object.values(state.filters).some((value) => value !== 'all');
  return Boolean(state.query || hasFilter || state.sort !== defaults.sort || state.dir !== defaults.dir);
}

function applyBrowseState(grid: HTMLElement, controls: HTMLElement, state: BrowseState, defaults: BrowseState, mode?: HistoryMode) {
  const search = controls.querySelector<HTMLInputElement>('[data-browse-search]');
  const status = controls.querySelector<HTMLElement>('[data-browse-status]');
  const reset = controls.querySelector<HTMLButtonElement>('[data-browse-reset]');
  const empty = grid.closest('main')?.querySelector<HTMLElement>('[data-browse-empty]');
  const singular = controls.dataset.browseSingular || 'item';
  const plural = controls.dataset.browsePlural || 'items';
  const query = normalize(state.query);

  if (search && search.value !== state.query) {
    search.value = state.query;
  }

  updateFilterButtons(controls, state);
  const grouping = layoutGrid(grid, controls, state, defaults);
  updateSortButtons(controls, state);

  let visibleCount = 0;
  const items = getItems(grid);

  items.forEach((item) => {
    let visible = true;

    if (query) {
      visible = normalize(item.dataset.search).includes(query);
    }

    if (visible) {
      Object.entries(state.filters).forEach(([key, value]) => {
        if (!visible || value === 'all') return;
        const itemValues = normalize(item.dataset[key]).split(',').map((part) => part.trim());
        visible = itemValues.includes(value);
      });
    }

    item.hidden = !visible;
    if (visible) visibleCount += 1;
  });

  // When grouping, hide any letter divider whose items are all filtered out.
  if (grouping) {
    const visibleByLetter = new Map<string, number>();
    items.forEach((item) => {
      if (item.hidden) return;
      const letter = item.dataset.letter || '';
      visibleByLetter.set(letter, (visibleByLetter.get(letter) || 0) + 1);
    });
    getDividers(grid).forEach((divider) => {
      divider.hidden = !visibleByLetter.get(divider.dataset.letter || '');
    });
  }

  const totalCount = items.length;
  const itemLabel = getItemLabel(visibleCount, singular, plural);
  if (status) {
    status.textContent = `${visibleCount} of ${totalCount} ${itemLabel} shown`;
  }
  if (empty) {
    empty.hidden = visibleCount !== 0;
  }
  if (reset) {
    reset.hidden = !hasActiveState(state, defaults);
  }

  if (mode) setUrl(state, defaults, mode);
}

function initBrowseControls() {
  const grid = document.querySelector<HTMLElement>('[data-browse-grid]');
  if (!grid || initializedGrids.has(grid)) return;

  const controls = getControls(grid);
  if (!controls) return;

  initializedGrids.add(grid);

  const defaults: BrowseState = {
    filters: {},
    query: '',
    sort: controls.dataset.defaultSort || '',
    dir: controls.dataset.defaultDir === 'desc' ? 'desc' : 'asc',
  };

  controls.querySelectorAll<HTMLElement>('[data-filter-key]').forEach((group) => {
    const key = group.dataset.filterKey;
    if (key) defaults.filters[key] = 'all';
  });

  let state = readStateFromUrl(controls);
  applyBrowseState(grid, controls, state, defaults);

  controls.querySelector<HTMLInputElement>('[data-browse-search]')?.addEventListener('input', (event) => {
    state = { ...state, query: (event.currentTarget as HTMLInputElement).value };
    applyBrowseState(grid, controls, state, defaults, 'replace');
  });

  controls.querySelector<HTMLInputElement>('[data-browse-search]')?.addEventListener('change', (event) => {
    state = { ...state, query: (event.currentTarget as HTMLInputElement).value };
    applyBrowseState(grid, controls, state, defaults, 'push');
  });

  controls.querySelectorAll<HTMLButtonElement>('[data-filter-value]').forEach((button) => {
    button.addEventListener('click', () => {
      const group = button.closest<HTMLElement>('[data-filter-key]');
      const key = group?.dataset.filterKey;
      if (!key) return;

      state = {
        ...state,
        filters: {
          ...state.filters,
          [key]: normalize(button.dataset.filterValue) || 'all',
        },
      };
      applyBrowseState(grid, controls, state, defaults, 'push');
    });
  });

  controls.querySelectorAll<HTMLButtonElement>('[data-sort]').forEach((button) => {
    button.addEventListener('click', () => {
      const sort = button.dataset.sort || defaults.sort;
      const nextDir = state.sort === sort
        ? (state.dir === 'asc' ? 'desc' : 'asc')
        : (button.dataset.dir === 'asc' ? 'asc' : 'desc');

      state = { ...state, sort, dir: nextDir };
      applyBrowseState(grid, controls, state, defaults, 'push');
    });
  });

  controls.querySelector<HTMLButtonElement>('[data-browse-reset]')?.addEventListener('click', () => {
    state = {
      filters: { ...defaults.filters },
      query: '',
      sort: defaults.sort,
      dir: defaults.dir,
    };
    applyBrowseState(grid, controls, state, defaults, 'push');
  });

  window.addEventListener('popstate', () => {
    state = readStateFromUrl(controls);
    applyBrowseState(grid, controls, state, defaults);
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initBrowseControls);
} else {
  initBrowseControls();
}
