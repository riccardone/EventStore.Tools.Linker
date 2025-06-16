using Linker.Core;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Tests;

public class FilterServiceTests
{
    [Test]
    public void can_include_events_using_stream_filters()
    {
        // PREPARE
        var prefix1 = "Stream1Prefix-";
        var prefix2 = "Stream2Prefix-";

        var streamFilter1 = new Filter(FilterType.Stream, $"{prefix1}*", FilterOperation.Include);
        var streamFilter2 = new Filter(FilterType.Stream, $"{prefix2}*", FilterOperation.Include);

        var sut = new FilterService(streamFilter2,streamFilter1);

        // RUN
        var valid1 = sut.IsValid("some type", $"{prefix1}1222");
        var valid2 = sut.IsValid("some type", $"{prefix2}1222");
        var invalid = sut.IsValid("some type", "other stream");

        // ASSERT
        ClassicAssert.True(valid1);
        ClassicAssert.True(valid2);
        ClassicAssert.False(invalid);
    }

    [Test]
    public void can_include_events_using_eventtype_filters()
    {
        // PREPARE
        var prefix1 = "Event1Prefix-";
        var prefix2 = "Event2Prefix-";

        var eventFilter1 = new Filter(FilterType.EventType, $"{prefix1}*", FilterOperation.Include);
        var eventFilter2 = new Filter(FilterType.EventType, $"{prefix2}*", FilterOperation.Include);

        var sut = new FilterService(eventFilter1,eventFilter2);

        // RUN
        var valid1 = sut.IsValid($"{prefix1}1222", "stream1");
        var valid2 = sut.IsValid($"{prefix1}1222", "stream1");
        var invalid = sut.IsValid("otherEvent", "stream1");

        // ASSERT
        ClassicAssert.True(valid1);
        ClassicAssert.True(valid2);
        ClassicAssert.False(invalid);
    }

    [Test]
    public void can_include_events_using_eventtype_and_stream_filters()
    {
        // PREPARE
        var prefix = "someprefix-";

        var streamFilter = new Filter(FilterType.EventType, $"{prefix}*", FilterOperation.Include);
        var eventFilter = new Filter(FilterType.Stream, $"{prefix}*", FilterOperation.Include);

        var sut = new FilterService(streamFilter, eventFilter);

        // RUN
        var valid1 = sut.IsValid($"{prefix}1222", "stream1");
        var valid2 = sut.IsValid("someType", $"{prefix}3322");
        var invalid = sut.IsValid("otherEvent", "otherStream");

        // ASSERT
        ClassicAssert.True(valid1);
        ClassicAssert.True(valid2);
        ClassicAssert.False(invalid);
    }

    [Test]
    public void can_exclude_events_using_stream_filters()
    {
        // PREPARE
        var prefix1 = "Stream1Prefix-";
        var prefix2 = "Stream2Prefix-";

        var streamFilter1 = new Filter(FilterType.Stream, $"{prefix1}*", FilterOperation.Exclude);
        var streamFilter2 = new Filter(FilterType.Stream, $"{prefix2}*", FilterOperation.Exclude);

        var sut = new FilterService(streamFilter2,streamFilter1);

        // RUN
        var invalid1 = sut.IsValid("some type", $"{prefix1}1222");
        var invalid2 = sut.IsValid("some type", $"{prefix2}1222");
        var valid = sut.IsValid("some type", "other stream");

        // ASSERT
        ClassicAssert.False(invalid1);
        ClassicAssert.False(invalid2);
        ClassicAssert.True(valid);
    }

    [Test]
    public void can_exclude_events_using_eventtype_filters()
    {
        // PREPARE
        var prefix1 = "Event1Prefix-";
        var prefix2 = "Event2Prefix-";

        var eventFilter1 = new Filter(FilterType.EventType, $"{prefix1}*", FilterOperation.Exclude);
        var eventFilter2 = new Filter(FilterType.EventType, $"{prefix2}*", FilterOperation.Exclude);

        var sut = new FilterService(eventFilter1,eventFilter2);

        // RUN
        var invalid1 = sut.IsValid($"{prefix1}1222", "stream1");
        var invalid2 = sut.IsValid($"{prefix1}1222", "stream1");
        var valid = sut.IsValid("otherEvent", "stream1");

        // ASSERT
        ClassicAssert.False(invalid1);
        ClassicAssert.False(invalid2);
        ClassicAssert.True(valid);
    }

    [Test]
    public void can_exclude_events_using_eventtype_and_stream_filters()
    {
        // PREPARE
        var prefix = "someprefix-";

        var streamFilter = new Filter(FilterType.EventType, $"{prefix}*", FilterOperation.Exclude);
        var eventFilter = new Filter(FilterType.Stream, $"{prefix}*", FilterOperation.Exclude);

        var sut = new FilterService(streamFilter, eventFilter);

        // RUN
        var invalid1 = sut.IsValid($"{prefix}1222", "stream1");
        var invalid2 = sut.IsValid("someType", $"{prefix}3322");
        var valid = sut.IsValid("otherEvent", "otherStream");

        // ASSERT
        ClassicAssert.False(invalid1);
        ClassicAssert.False(invalid2);
        ClassicAssert.True(valid);
    }

    [Test]
    public void can_filter_using_include_and_exclude_stream_filters()
    {
        // PREPARE
        var prefix = "SomePrefix-";
        var postfix = "SomePostfix-";

        var includeFilter = new Filter(FilterType.Stream, $"{prefix}*", FilterOperation.Include);
        var excludeFilter = new Filter(FilterType.Stream, $"{prefix}-{postfix}*", FilterOperation.Exclude);
            
        var sut = new FilterService(includeFilter, excludeFilter);

        // RUN
        var valid = sut.IsValid("some type", $"{prefix}1222");
        var invalid1 = sut.IsValid("some type", $"{prefix}-{postfix}1222");
        var invalid2 = sut.IsValid("some type", "other stream");

        // ASSERT
        ClassicAssert.False(invalid1);
        ClassicAssert.False(invalid2);
        ClassicAssert.True(valid);
    }

    [Test]
    public void can_filter_using_include_and_exclude_eventtype_filters()
    {
        // PREPARE
        var prefix = "SomePrefix-";
        var postfix = "SomePostfix-";

        var includeFilter = new Filter(FilterType.EventType, $"{prefix}*", FilterOperation.Include);
        var excludeFilter = new Filter(FilterType.EventType, $"{prefix}-{postfix}*", FilterOperation.Exclude);
            
        var sut = new FilterService(includeFilter, excludeFilter);

        // RUN
        var valid = sut.IsValid($"{prefix}1222", "stream");
        var invalid1 = sut.IsValid($"{prefix}-{postfix}1222", "stream");
        var invalid2 = sut.IsValid("some type", "other stream");

        // ASSERT
        ClassicAssert.False(invalid1);
        ClassicAssert.False(invalid2);
        ClassicAssert.True(valid);
    }
}