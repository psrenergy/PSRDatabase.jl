function year_hour_to_month(year_hour::Integer)
    for i in 1:length(DAYS_IN_MONTH)
        if 0 < year_hour && year_hour <= sum(DAYS_IN_MONTH[1:i]) * 24
            return i
        end
    end
    error("Invalid year hour")
    return nothing
end
