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

function sortGrid(grid: HTMLElement, state: BrowseState) {
  if (!state.sort) return;

  const items = Array.from(grid.children) as HTMLElement[];
  const multiplier = state.dir === 'asc' ? 1 : -1;

  items.sort((a, b) => {
    if (state.sort === 'title') {
      return multiplier * (a.dataset.title || '').localeCompare(b.dataset.title || '');
    }

    const aValue = parseInt(a.dataset[state.sort] || '0', 10) || 0;
    const bValue = parseInt(b.dataset[state.sort] || '0', 10) || 0;
    return multiplier * (aValue - bValue);
  });

  items.forEach((item) => grid.appendChild(item));
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
  sortGrid(grid, state);
  updateSortButtons(controls, state);

  let visibleCount = 0;
  const items = Array.from(grid.children) as HTMLElement[];

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
