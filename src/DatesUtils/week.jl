function week_to_epoch(week::Integer, year::Integer)
    if week < 10
        return date_to_epoch(1, 1, year) + (week - 1) * EPOCH_WEEK
    else
        return date_to_epoch(5, 3, year) + (week - 10) * EPOCH_WEEK
    end
end

function epoch_to_week(epoch::Integer)
    datetime = epoch_to_datetime(epoch)
    month = Dates.month(datetime)
    year = Dates.year(datetime)
    days =
        if month < 3
            days_difference(datetime, DateTime(year, 1, 1))
        else
            days_difference(datetime, DateTime(year, 3, 1)) + 31 + 28
        end

    return min(floor(Int, days / 7) + 1, 52)
end

function stage_to_week(reference::DateReference, stage::Integer)
    initial_stage = reference.initial_stage
    initial_year = reference.initial_year
    current_stage = initial_stage + stage - 2

    week = mod(current_stage, 52) + 1
    year = floor(Int, current_stage / 52) + initial_year

    return week, year
end
