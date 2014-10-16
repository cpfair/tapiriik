class StreamSampler:
    def SampleWithCallback(callback, streams):
        """
            *streams should be a dict in format {"stream1":[(ts1,val1), (ts2, val2)...]...} where ts is a numerical offset from the activity start.
            Expect callback(time_offset, stream1=value1, stream2=value2) in chronological order. Stream values may be None
            All samples are represented - none are dropped
        """

        # Collate the individual streams into discrete waypoints.
        # There is no global sampling rate - waypoints are created for every new datapoint in any stream (simultaneous datapoints are included in the same waypoint)
        # Resampling is based on the last known value of the stream - no interpolation or nearest-neighbour.

        streamData = streams
        streams = list(streams.keys())
        print("Handling streams %s" % streams)

        stream_indices = dict([(stream, -1) for stream in streams]) # -1 meaning the stream has yet to start
        stream_lengths = dict([(stream, len(streamData[stream])) for stream in streams])

        currentTimeOffset = 0

        while True:
            advance_stream = None
            advance_offset = None
            for stream in streams:
                if stream_indices[stream] + 1 == stream_lengths[stream]:
                    continue # We're at the end - can't advance
                if advance_offset is None or streamData[stream][stream_indices[stream] + 1][0] - currentTimeOffset < advance_offset:
                    advance_offset = streamData[stream][stream_indices[stream] + 1][0] - currentTimeOffset
                    advance_stream = stream
            if not advance_stream:
                break # We've hit the end of every stream, stop
            # Update the current time offset based on the key advancing stream (others may still be behind)
            currentTimeOffset = streamData[advance_stream][stream_indices[advance_stream] + 1][0]
            # Advance streams with the current timestamp, including advance_stream
            for stream in streams:
                if stream_indices[stream] + 1 == stream_lengths[stream]:
                    continue # We're at the end - can't advance
                if streamData[stream][stream_indices[stream] + 1][0] == currentTimeOffset: # Don't need to consider <, as then that stream would be advance_stream
                    stream_indices[stream] += 1
            callbackDataArgs = {}
            for stream in streams:
                if stream_indices[stream] >= 0:
                    callbackDataArgs[stream] = streamData[stream][stream_indices[stream]][1]
            callback(currentTimeOffset, **callbackDataArgs)
