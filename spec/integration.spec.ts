import { tap, mapTo } from 'rxjs/operators';

import { of, timer, Subject, concat } from 'rxjs';

import {
  cold,
  getTestScheduler,
  hot,
  initTestScheduler,
  resetTestScheduler,
  time,
} from '../index';

describe('Integration', () => {
  it('should work with a cold observable', () => {
    const provided = of(1);

    const expected = cold('(b|)', { b: 1 });

    expect(provided).toBeObservable(expected);
  });

  it('should work with a hot observable', () => {
    const provided = new Subject<number>();

    const expected = hot('--a--b', { a: 1, b: 2 });

    expect(expected.pipe(tap((v) => provided.next(v)))).toBeObservable(
      expected,
    );
  });

  it('should trim spaces of marble string, if any', () => {
    const source = hot('  -a-^(bc)-|  ');
    const expected = cold('  -(bc)-|  ');

    expect(source).toBeObservable(expected);
  });

  it('should support testing subscriptions on hot observable', () => {
    const source = hot('-a-^b---c-|');
    const expected = cold('-b---c-|');
    const subscription = '^------!';

    expect(source).toBeObservable(expected);
    expect(source).toHaveSubscriptions(subscription);
  });

  it('should identify subscription points', () => {
    const obs1 = cold('-a---b-|');
    const obs2 = cold('-c---d-|');
    const expected = cold('-a---b--c---d-|');
    const sub1 = '^------!';
    const sub2 = '-------^------!';

    expect(concat(obs1, obs2)).toBeObservable(expected);
    expect(obs1).toHaveSubscriptions(sub1);
    expect(obs2).toHaveSubscriptions(sub2);
  });

  it('should work with the test scheduler', () => {
    const delay = time('-----a|');
    const val = 1;
    const provided = timer(delay, getTestScheduler()).pipe(mapTo(val));

    const expected = hot('------(a|)', { a: val });

    expect(provided).toBeObservable(expected);
  });

  it('should throw if the TestScheduler is not initialized', () => {
    resetTestScheduler();

    try {
      getTestScheduler();
    } catch (err) {
      expect(err).toEqual(new Error('No test scheduler initialized'));
    }

    initTestScheduler();
  });

  it('should support time progression syntax for hot', () => {
    const provided = timer(100, getTestScheduler()).pipe(mapTo('a'));
    const expected = hot('100ms (a|)');

    expect(provided).toBeObservable(expected);
  });

  it('should support time progression syntax for cold', () => {
    const provided = timer(100, getTestScheduler()).pipe(mapTo('a'));
    const expected = cold('100ms (a|)');

    expect(provided).toBeObservable(expected);
  });

  it('should support TestScheduler.run()', () => {
    const scheduler = getTestScheduler();

    scheduler.run(({ expectObservable }) => {
      const delay = time('-----a|');
      const val = 1;
      const provided = timer(delay, scheduler).pipe(mapTo(val));

      expectObservable(provided).toBe('------(a|)', { a: val });
    });
  });

  it('should support "not.toBeObservable"', () => {
    const provided = of(1);

    const expected = cold('(b|)', { b: 2 });

    expect(provided).not.toBeObservable(expected);
  });

  it('should support jasmine.anything()', () => {
    const provided = cold('a', { a: { someProp: 3 } });
    const expected = cold('a', { a: jasmine.anything() });

    expect(provided).toBeObservable(expected);
  });
});
