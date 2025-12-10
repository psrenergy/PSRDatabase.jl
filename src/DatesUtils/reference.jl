struct DateReference
    stage_type::StageType.T
    initial_stage::Int
    initial_year::Int

    function DateReference(stage_type::StageType.T, initial_stage::Integer, initial_year::Integer)
        if stage_type == StageType.WEEK
            if initial_stage < 1 || initial_stage > 52
                error("Invalid initial stage")
            end
        elseif stage_type == StageType.MONTH
            if initial_stage < 1 || initial_stage > 12
                error("Invalid initial stage")
            end
        elseif stage_type == StageType.YEAR
            if initial_stage != 1
                error("Invalid initial stage")
            end
        else
            error("Invalid stage type")
        end

        return new(stage_type, initial_stage, initial_year)
    end

    function DateReference(stage_type::StageType.T, initial_year::Integer)
        return DateReference(stage_type, 1, initial_year)
    end
end

function stage_to_date(reference::DateReference, stage::Integer)
    stage_type = reference.stage_type

    if stage_type == StageType.WEEK
        return stage_to_week(reference, stage)
    elseif stage_type == StageType.MONTH
        return stage_to_month(reference, stage)
    elseif stage_type == StageType.YEAR
        return 1, stage_to_year(reference, stage)
    else
        error("Invalid stage type")
    end

    return 0
end

function stage_to_epoch(reference::DateReference, stage::Integer)
    stage_type = reference.stage_type

    if stage_type == StageType.WEEK
        week, year = stage_to_week(reference, stage)
        return week_to_epoch(week, year)
    elseif stage_type == StageType.MONTH
        month, year = stage_to_month(reference, stage)
        return month_to_epoch(month, year)
    elseif stage_type == StageType.YEAR
        year = stage_to_year(reference, stage)
        return year_to_epoch(year)
    else
        error("Invalid stage type")
    end

    return 0
end

function epoch_to_stage(reference::DateReference, epoch::Integer)
    stage_type = reference.stage_type

    if stage_type == StageType.WEEK
        return epoch_to_week(epoch)
    elseif stage_type == StageType.MONTH
        return epoch_to_month(epoch)
    elseif stage_type == StageType.YEAR
        return epoch_to_year(epoch)
    else
        error("Invalid stage type")
    end

    return 0
end
