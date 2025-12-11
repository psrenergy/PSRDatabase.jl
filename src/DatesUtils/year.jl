function is_leap_year(year::Integer)
    return Dates.isleapyear(year)
end

function year_to_epoch(year::Integer)
    return date_to_epoch(1, 1, year)
end

function year_hour_to_epoch(year::Integer, hour::Integer)
    return date_to_epoch(1, 1, year) + hour * EPOCH_HOUR
end

function epoch_to_year(epoch::Integer)
    datetime = epoch_to_datetime(epoch)
    return Dates.year(datetime)
end

function stage_to_year(reference::DateReference, stage::Integer)
    initial_year = reference.initial_year
    year = stage + initial_year - 1
    return year
end
