function month_to_epoch(month::Integer, year::Integer)
    return date_to_epoch(1, month, year)
end

function epoch_to_month(epoch::Integer)
    datetime = epoch_to_datetime(epoch)
    return Dates.month(datetime)
end

function stage_to_month(reference::DateReference, stage::Integer)
    initial_stage = reference.initial_stage
    initial_year = reference.initial_year
    current_stage = initial_stage + stage - 2

    month = mod(current_stage, 12) + 1
    year = floor(Int, current_stage / 12) + initial_year

    return month, year
end
