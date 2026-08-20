using EIE.ServiceBus.AgeMonitor.Configuration;
using EIE.ServiceBus.AgeMonitor.Domain;
using EIE.ServiceBus.AgeMonitor.Measurement;
using EIE.ServiceBus.AgeMonitor.Tests.Support;

namespace EIE.ServiceBus.AgeMonitor.Tests;

/// <summary>Spec §11.1 — measurement logic, TST-001..014.</summary>
public sealed class MeasurementTests
{
    private readonly MonitorOptions _options = new();
    private readonly TestClock _clock = new(Build.T0);

    private (OldestActiveMessageResolver Resolver, FakePeeker Peeker, EIE.ServiceBus.AgeMonitor.State.ScanStateStore State) Sut(
        params PeekedMessage[] messages)
    {
        var peeker = new FakePeeker().With("q-test", messages);
        var state = Build.StateStore(_options);
        var degradation = Build.Degradation(_options);
        return (Build.Resolver(peeker, state, degradation, _options, _clock), peeker, state);
    }

    private static EntityRuntimeInfo Runtime(long active) =>
        new(active, 0, 0, 0, Build.T0);

    [Fact(DisplayName = "TST-001 single Active message 400s old is Measured with age ~400")]
    public async Task Tst001_SingleActiveMessage()
    {
        var (resolver, _, _) = Sut(Build.Message(1, Build.T0.AddSeconds(-400)));

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(1), 5000, default);

        Assert.Equal(MeasurementStatus.Measured, result.Status);
        Assert.NotNull(result.OldestActive);
        Assert.Equal(400d, (Build.T0 - result.OldestActive!.EnqueuedTimeUtc).TotalSeconds);
    }

    [Fact(DisplayName = "TST-002 head Deferred with Active at position 300 measures the Active message")]
    public async Task Tst002_DeferredHeadActiveBehind()
    {
        var messages = Enumerable.Range(1, 300)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-1000), MessageState.Deferred))
            .Append(Build.Message(301, Build.T0.AddSeconds(-250)))
            .ToArray();

        var (resolver, _, _) = Sut(messages);

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(301), 5000, default);

        Assert.Equal(MeasurementStatus.Measured, result.Status);
        Assert.Equal(301, result.OldestActive!.SequenceNumber);
        Assert.Equal(301, result.MessagesExamined);
    }

    [Fact(DisplayName = "TST-003 all messages Deferred within budget yields HeadBlockedByDeferred and no age")]
    public async Task Tst003_AllDeferredWithinBudget()
    {
        var messages = Enumerable.Range(1, 100)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred))
            .ToArray();

        var (resolver, _, _) = Sut(messages);

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(100), 5000, default);

        Assert.Equal(MeasurementStatus.HeadBlockedByDeferred, result.Status);
        Assert.Null(result.OldestActive);
    }

    [Fact(DisplayName = "TST-004 scan budget exhaustion uses exactly depth/batch peeks")]
    public async Task Tst004_BudgetExhausted()
    {
        var messages = Enumerable.Range(1, 6000)
            .Select(i => Build.Message(i, Build.T0.AddSeconds(-900), MessageState.Deferred))
            .ToArray();

        var (resolver, peeker, _) = Sut(messages);

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(6000), 5000, default);

        Assert.Equal(MeasurementStatus.HeadBlockedByDeferred, result.Status);
        Assert.Equal(5000 / 250, result.BatchesUsed);
        Assert.Equal(5000 / 250, peeker.PeekCallCount);
        Assert.Equal(5000, result.MessagesExamined);
    }

    [Fact(DisplayName = "TST-005 Scheduled message with a future enqueue time is skipped")]
    public async Task Tst005_ScheduledSkipped()
    {
        var (resolver, _, _) = Sut(
            Build.Message(1, Build.T0.AddSeconds(-800), MessageState.Scheduled, scheduledEnqueue: Build.T0.AddMinutes(30)),
            Build.Message(2, Build.T0.AddSeconds(-500)));

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(2), 5000, default);

        Assert.Equal(MeasurementStatus.Measured, result.Status);
        Assert.Equal(2, result.OldestActive!.SequenceNumber);
    }

    [Fact(DisplayName = "TST-006 expired-but-not-collected message is skipped")]
    public async Task Tst006_ExpiredSkipped()
    {
        var (resolver, _, _) = Sut(
            Build.Message(1, Build.T0.AddSeconds(-900), ttl: TimeSpan.FromSeconds(60)),
            Build.Message(2, Build.T0.AddSeconds(-400), ttl: TimeSpan.FromHours(4)));

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(2), 5000, default);

        Assert.Equal(MeasurementStatus.Measured, result.Status);
        Assert.Equal(2, result.OldestActive!.SequenceNumber);
    }

    [Fact(DisplayName = "TST-007 mixed Deferred, Scheduled and expired with one trailing Active is Measured")]
    public async Task Tst007_MixedStates()
    {
        var (resolver, _, _) = Sut(
            Build.Message(1, Build.T0.AddSeconds(-900), MessageState.Deferred),
            Build.Message(2, Build.T0.AddSeconds(-880), MessageState.Scheduled, scheduledEnqueue: Build.T0.AddMinutes(5)),
            Build.Message(3, Build.T0.AddSeconds(-870), ttl: TimeSpan.FromSeconds(30)),
            Build.Message(4, Build.T0.AddSeconds(-310)));

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(4), 5000, default);

        Assert.Equal(MeasurementStatus.Measured, result.Status);
        Assert.Equal(4, result.OldestActive!.SequenceNumber);
    }

    [Fact(DisplayName = "TST-008 empty entity whose previous tick was a trusted Empty stays Empty")]
    public async Task Tst008_EmptyAfterTrustedEmpty()
    {
        var (resolver, _, state) = Sut();
        state.RecordObservation("q-test", MeasurementStatus.Empty, null, 0d, 0);

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(0), 5000, default);

        Assert.Equal(MeasurementStatus.Empty, result.Status);
    }

    [Fact(DisplayName = "TST-009 empty entity after a prior depth of 500 is Indeterminate, not healthy")]
    public async Task Tst009_AbruptDropIsIndeterminate()
    {
        var (resolver, _, state) = Sut();
        state.RecordObservation("q-test", MeasurementStatus.Measured, 42, 120d, 500);

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(0), 5000, default);

        Assert.Equal(MeasurementStatus.Indeterminate, result.Status);
    }

    [Fact(DisplayName = "TST-010 empty result while a throttle was observed this tick is Indeterminate")]
    public async Task Tst010_ThrottleMakesZeroUntrustworthy()
    {
        var peeker = new FakePeeker().With("q-test", []);
        var state = Build.StateStore(_options);
        var degradation = Build.Degradation(_options);

        degradation.BeginTick();
        degradation.RecordThrottle("q-other");

        var resolver = Build.Resolver(peeker, state, degradation, _options, _clock);

        var result = await resolver.ResolveAsync(Build.Queue(), Runtime(0), 5000, default);

        Assert.Equal(MeasurementStatus.Indeterminate, result.Status);
    }

    [Fact(DisplayName = "TST-011 three consecutive clean zeros promote to Empty")]
    public async Task Tst011_ZeroStreakPromotesToEmpty()
    {
        var (resolver, _, state) = Sut();
        var entity = Build.Queue();

        var first = await resolver.ResolveAsync(entity, Runtime(0), 5000, default);
        state.RecordObservation(entity.EntityPath, first.Status, null, null, 0);

        var second = await resolver.ResolveAsync(entity, Runtime(0), 5000, default);
        state.RecordObservation(entity.EntityPath, second.Status, null, null, 0);

        var third = await resolver.ResolveAsync(entity, Runtime(0), 5000, default);

        Assert.Equal(MeasurementStatus.Indeterminate, first.Status);
        Assert.Equal(MeasurementStatus.Indeterminate, second.Status);
        Assert.Equal(MeasurementStatus.Empty, third.Status);
    }

    [Fact(DisplayName = "TST-011b a cold monitor never reaches the Indeterminate alert threshold on an idle queue")]
    public async Task Tst011b_ColdStartDoesNotTripIndeterminateAlert()
    {
        // The zero-corroboration and Indeterminate-alert thresholds are both 3, so the
        // promotion must land before the alert would fire; otherwise every idle entity
        // pages on the first tick after a restart.
        var (resolver, _, state) = Sut();
        var entity = Build.Queue();
        var consecutiveIndeterminate = 0;

        for (var tick = 0; tick < 5; tick++)
        {
            var result = await resolver.ResolveAsync(entity, Runtime(0), 5000, default);
            state.RecordObservation(entity.EntityPath, result.Status, null, null, 0);

            consecutiveIndeterminate = result.Status == MeasurementStatus.Indeterminate
                ? consecutiveIndeterminate + 1
                : 0;

            Assert.True(
                consecutiveIndeterminate < _options.IndeterminateTicksForAlert,
                $"tick {tick} reached {consecutiveIndeterminate} consecutive Indeterminate results");
        }
    }

    [Fact(DisplayName = "TST-012 an enqueue time in the future clamps age to zero and flags ClockAnomaly")]
    public void Tst012_FutureEnqueueTimeIsClockAnomaly()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            Runtime(1),
            new AgeResolution(MeasurementStatus.Measured, Build.Message(1, Build.T0.AddSeconds(10)), 1, 1),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        Assert.Equal(MeasurementStatus.ClockAnomaly, record.Status);
        Assert.Equal(0d, record.AgeSeconds);
        Assert.Null(record.AgeBreachRatioSev1);
        Assert.Null(record.AgeBreachRatioSev2);
    }

    [Fact(DisplayName = "TST-012b sub-tolerance clock jitter clamps without raising ClockAnomaly")]
    public void Tst012b_SubToleranceJitterIsNotAnomalous()
    {
        // Flagging every sub-second NTP excursion would keep ALR-019 firing permanently
        // across all entities. See SPEC-DEVIATION-001.
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            Runtime(1),
            new AgeResolution(MeasurementStatus.Measured, Build.Message(1, Build.T0.AddMilliseconds(500)), 1, 1),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        Assert.Equal(MeasurementStatus.Measured, record.Status);
        Assert.Equal(0d, record.AgeSeconds);
    }

    [Fact(DisplayName = "TST-013 an implausible age is recorded but never alertable")]
    public void Tst013_ImplausibleAgeRecordedNotAlerted()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            Runtime(1),
            new AgeResolution(MeasurementStatus.Measured, Build.Message(1, Build.T0.AddDays(-45)), 1, 1),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        Assert.Equal(MeasurementStatus.ClockAnomaly, record.Status);
        Assert.True(record.AgeSeconds > _options.ImplausibleAgeSeconds);
        Assert.Null(record.AgeBreachRatioSev1);
        Assert.Null(record.AgeBreachRatioSev2);
    }

    [Theory(DisplayName = "TST-014 non-measurement statuses emit no ratio point, never a zero")]
    [InlineData(MeasurementStatus.Indeterminate)]
    [InlineData(MeasurementStatus.HeadBlockedByDeferred)]
    [InlineData(MeasurementStatus.NotMeasured)]
    [InlineData(MeasurementStatus.Throttled)]
    [InlineData(MeasurementStatus.EntityNotFound)]
    [InlineData(MeasurementStatus.Empty)]
    public void Tst014_NoRatioForNonMeasurement(MeasurementStatus status)
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            Runtime(0),
            new AgeResolution(status, null, 0, 0),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        Assert.Null(record.AgeBreachRatioSev1);
        Assert.Null(record.AgeBreachRatioSev2);
    }

    [Fact(DisplayName = "TST-014b a Measured record carries both ratios against the effective thresholds")]
    public void Tst014b_MeasuredCarriesRatios()
    {
        var state = Build.StateStore(_options);
        var factory = Build.Factory(state, _options, _clock);

        var record = factory.Create(new MeasurementInputs(
            Build.Queue(),
            Runtime(5),
            new AgeResolution(MeasurementStatus.Measured, Build.Message(1, Build.T0.AddSeconds(-450)), 1, 1),
            new ThresholdSet(300, 900, ThresholdSource.AppConfig),
            DegradationLevel.L0,
            "tick",
            Build.T0,
            null));

        Assert.Equal(1.5d, record.AgeBreachRatioSev2!.Value, 6);
        Assert.Equal(0.5d, record.AgeBreachRatioSev1!.Value, 6);
        Assert.Equal(300, record.Thresholds.Sev2Seconds);
        Assert.Equal(900, record.Thresholds.Sev1Seconds);
    }
}
