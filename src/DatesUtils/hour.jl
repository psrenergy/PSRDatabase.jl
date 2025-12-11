function hours_in_month(month::Integer)
    if month < 1 || month > 12
        error("Invalid month")
    end
    return 24 * DAYS_IN_MONTH[month]
end
