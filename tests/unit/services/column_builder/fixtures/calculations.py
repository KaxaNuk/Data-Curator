def c_sum_mhigh_and_mlow(
    m_high,
    m_low
):
    return m_high + m_low


def c_multiply_sum_by_mclose(
    c_sum_mhigh_and_mlow,
    m_close
):
    return c_sum_mhigh_and_mlow * m_close


def c_failing_calculation(
    m_close
):
    msg = "Test calculation error"

    raise ValueError(msg)


def c_wrong_length_calculation(
    m_close
):
    # One row more than the m_close column it gets
    return [
        *m_close.to_pyarrow().to_pylist(),
        1.0,
    ]
