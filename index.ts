import {
  CompleteNotification,
  ErrorNotification,
  NextNotification,
  Observable,
  ObservableNotification,
  Subscription,
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';

import {
  getTestScheduler,
  initTestScheduler,
  resetTestScheduler,
} from './src/scheduler';
import {
  TestColdObservable,
  TestHotObservable,
  TestObservable,
} from './src/test-observables';
import { unparseMarble } from './src/marble-unparser';
import { mapSymbolsToNotifications } from './src/map-symbols-to-notifications';
import { TestMessages } from './src/types';

export {
  getTestScheduler,
  initTestScheduler,
  resetTestScheduler,
} from './src/scheduler';

export function hot(
  marbles: string,
  values?: any,
  error?: any,
): TestHotObservable {
  return new TestHotObservable(marbles.trim(), values, error);
}

export function cold(
  marbles: string,
  values?: any,
  error?: any,
): TestColdObservable {
  return new TestColdObservable(marbles.trim(), values, error);
}

export function time(marbles: string): number {
  return getTestScheduler().createTime(marbles.trim());
}

declare global {
  namespace jasmine {
    interface Matchers<T> {
      toBeObservable(expected: TestObservable): boolean;
      toHaveSubscriptions(marbles: string | string[]): boolean;
    }
  }
  namespace jest {
    interface Matchers<R> {
      toBeObservable(expected: TestObservable): R;
      toHaveSubscriptions(marbles: string | string[]): R;
    }
  }
}

/*
 * Based on source code found in rxjs library
 * https://github.com/ReactiveX/rxjs/blob/master/src/testing/TestScheduler.ts
 *
 */
function materializeInnerObservable<T>(
  observable: Observable<T>,
  outerFrame: number,
): TestMessages {
  const messages: TestMessages = [];
  const scheduler = getTestScheduler();

  observable.subscribe({
    next: (value: any) => {
      messages.push({
        frame: scheduler.frame - outerFrame,
        notification: {
          kind: 'N',
          value,
          error: undefined,
        } as NextNotification<T>,
      });
    },
    error: (error: any) => {
      messages.push({
        frame: scheduler.frame - outerFrame,
        notification: {
          kind: 'E',
          value: undefined,
          error,
        } as ErrorNotification,
      });
    },
    complete: () => {
      messages.push({
        frame: scheduler.frame - outerFrame,
        notification: {
          kind: 'C',
          value: undefined,
          error: undefined,
        } as CompleteNotification,
      });
    },
  });
  return messages;
}

export function addMatchers() {
  jasmine.addMatchers({
    toHaveSubscriptions: () => ({
      compare: function (actual: TestObservable, marbles: string | string[]) {
        const marblesArray: string[] =
          typeof marbles === 'string' ? [marbles] : marbles;
        const results = marblesArray.map((marbles) =>
          TestScheduler.parseMarblesAsSubscriptions(marbles),
        );

        expect(results).toEqual(actual.getSubscriptions());

        return { pass: true };
      },
    }),
    toBeObservable: (utils, _equalityTester) => ({
      compare: function (actual: TestObservable, fixture: TestObservable) {
        const results: TestMessages = [];
        let subscription: Subscription;
        const scheduler = getTestScheduler();

        scheduler.schedule(() => {
          subscription = actual.subscribe({
            next: (x: any) => {
              let value = x;

              // Support Observable-of-Observables
              if (x instanceof Observable) {
                value = materializeInnerObservable(value, scheduler.frame);
              }

              results.push({
                frame: scheduler.frame,
                notification: {
                  kind: 'N',
                  value,
                  error: undefined,
                } as NextNotification<any>,
              });
            },
            error: (error: any) => {
              results.push({
                frame: scheduler.frame,
                notification: {
                  kind: 'E',
                  value: undefined,
                  error,
                } as ErrorNotification,
              });
            },
            complete: () => {
              results.push({
                frame: scheduler.frame,
                notification: {
                  kind: 'C',
                  value: undefined,
                  error: undefined,
                } as CompleteNotification,
              });
            },
          });
        });
        scheduler.flush();

        const expected = TestScheduler.parseMarbles(
          fixture.marbles,
          fixture.values,
          fixture.error,
          true,
          true,
        );

        if (utils.equals(results, expected)) {
          return { pass: true };
        }

        const mapNotificationToSymbol = buildNotificationToSymbolMapper(
          fixture.marbles,
          expected,
          (a: any, b: any) => utils.equals(a, b),
        );
        const receivedMarble = unparseMarble(results, mapNotificationToSymbol);

        const message = formatMessage(
          fixture.marbles,
          expected,
          receivedMarble,
          results,
        );
        return { pass: false, message };
      },
    }),
  });
}

function buildNotificationToSymbolMapper(
  expectedMarbles: string,
  expectedMessages: TestMessages,
  equalityFn: (a: any, b: any) => boolean,
) {
  const symbolsToNotificationsMap = mapSymbolsToNotifications(
    expectedMarbles,
    expectedMessages,
  );
  return (notification: ObservableNotification<any>) => {
    const mapped = Object.keys(symbolsToNotificationsMap).find((key) =>
      equalityFn(symbolsToNotificationsMap[key], notification),
    )!;

    return mapped || '?';
  };
}

function formatMessage(
  expectedMarbles: string,
  expectedMessages: TestMessages,
  receivedMarbles: string,
  receivedMessages: TestMessages,
) {
  return `
    Expected: ${expectedMarbles},
    Received: ${receivedMarbles},
    
    Expected:
    ${JSON.stringify(expectedMessages)}
    
    Received:
    ${JSON.stringify(receivedMessages)},
  `;
}

export function setupEnvironment() {
  beforeAll(() => addMatchers());

  beforeEach(() => initTestScheduler());
  afterEach(() => {
    getTestScheduler().flush();
    resetTestScheduler();
  });
}

setupEnvironment();
