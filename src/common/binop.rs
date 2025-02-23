use std::cmp::Ordering;

pub type BinopFunc = fn(left: f64, right: f64) -> f64;

/// eq returns true if left == right.
#[inline]
pub(crate) const fn op_eq(left: f64, right: f64) -> bool {
    // Special handling for nan == nan.
    if left.is_nan() {
        return right.is_nan();
    }
    left == right
}

/// neq returns true if left != right.
#[inline]
pub(crate) const fn op_neq(left: f64, right: f64) -> bool {
    // Special handling for comparison with nan.
    if left.is_nan() {
        return !right.is_nan();
    }
    if right.is_nan() {
        return true;
    }
    left != right
}

#[inline]
pub(crate) const fn op_and(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else {
        left
    }
}

// return the first non-NaN item. If both left and right are NaN, it returns NaN.
#[inline]
pub(crate) const fn op_or(left: f64, right: f64) -> f64 {
    if !left.is_nan() {
        return left;
    }
    right
}

/// gt returns true if left > right
#[inline]
pub(crate) const fn op_gt(left: f64, right: f64) -> bool {
    left > right
}

/// lt returns true if left < right
#[inline]
pub(crate) const fn op_lt(left: f64, right: f64) -> bool {
    left < right
}

/// Gte returns true if left >= right
#[inline]
pub(crate) const fn op_gte(left: f64, right: f64) -> bool {
    left >= right
}

/// Lte returns true if left <= right
#[inline]
pub(crate) const fn op_lte(left: f64, right: f64) -> bool {
    left <= right
}

/// Plus returns left + right
#[inline]
pub(crate) const fn op_plus(left: f64, right: f64) -> f64 {
    left + right
}

/// Minus returns left - right
#[inline]
pub(crate) const fn op_minus(left: f64, right: f64) -> f64 {
    left - right
}

/// Mul returns left * right
#[inline]
pub(crate) const fn op_mul(left: f64, right: f64) -> f64 {
    left * right
}

/// Div returns left / right
/// Todo: protect against div by zero
#[inline]
pub(crate) const fn op_div(left: f64, right: f64) -> f64 {
    left / right
}

/// returns left % right
#[inline]
pub(crate) const fn op_mod(left: f64, right: f64) -> f64 {
    left % right
}

/// pow returns pow(left, right)
#[inline]
pub(crate) fn op_pow(left: f64, right: f64) -> f64 {
    left.powf(right)
}

/// returns left or right if left is NaN.
#[inline]
pub(crate) const fn op_default(left: f64, right: f64) -> f64 {
    if left.is_nan() {
        return right;
    }
    left
}

/// returns left if right is not NaN. Otherwise, NaN is returned.
#[inline]
pub(crate) const fn op_if(left: f64, right: f64) -> f64 {
    if right.is_nan() {
        return f64::NAN;
    }
    left
}

/// if_not returns left if right is NaN. Otherwise, NaN is returned.
#[inline]
pub const fn op_if_not(left: f64, right: f64) -> f64 {
    if right.is_nan() {
        return left;
    }
    f64::NAN
}

#[inline]
pub const fn op_unless(left: f64, right: f64) -> f64 {
    if right != left {
        return f64::NAN;
    }
    left
}

/// convert true to x, false to NaN.
#[inline]
pub const fn to_comparison_value(b: bool, x: f64) -> f64 {
    if b {
        x
    } else {
        f64::NAN
    }
}

pub(crate) fn cmp(x: f64, y: f64) -> f64 {
    if x.is_nan() && y.is_nan() {
        return 1.0;
    }
    if x.is_nan() {
        return -1.0;
    }
    if y.is_nan() {
        return 1.0;
    }
    match x.total_cmp(&y) {
        Ordering::Less => -1.0,
        Ordering::Equal => 0.0,
        Ordering::Greater => 1.0,
    }
}

pub(crate) const fn min(x: f64, y: f64) -> f64 {
    x.min(y)
}

pub(crate) const fn max(x: f64, y: f64) -> f64 {
    x.max(y)
}

pub(crate) const fn avg(x: f64, y: f64) -> f64 {
    (x + y) / 2.0
}

pub(crate) const fn abs_diff(x: f64, y: f64) -> f64 {
    (x - y).abs()
}

pub(crate) const fn sgn_diff(x: f64, y: f64) -> f64 {
    (x - y).signum()
}

pub(crate) const fn pct_change(x: f64, y: f64) -> f64 {
    if x == 0.0 {
        return 0.0;
    }
    (y - x) / x
}

macro_rules! make_comparison_func {
    ($name: ident, $func: expr) => {
        #[inline]
        pub const fn $name(left: f64, right: f64) -> f64 {
            to_comparison_value($func(left, right), left)
        }
    };
}

macro_rules! make_comparison_func_bool {
    ($name: ident, $func: expr) => {
        #[inline]
        pub const fn $name(left: f64, right: f64) -> f64 {
            if left.is_nan() {
                return f64::NAN;
            }
            if $func(left, right) {
                1_f64
            } else {
                0_f64
            }
        }
    };
}

make_comparison_func!(compare_eq, op_eq);
make_comparison_func!(compare_neq, op_neq);
make_comparison_func!(compare_gt, op_gt);
make_comparison_func!(compare_lt, op_lt);
make_comparison_func!(compare_gte, op_gte);
make_comparison_func!(compare_lte, op_lte);

make_comparison_func_bool!(compare_eq_bool, op_eq);
make_comparison_func_bool!(compare_neq_bool, op_neq);
make_comparison_func_bool!(compare_gt_bool, op_gt);
make_comparison_func_bool!(compare_lt_bool, op_lt);
make_comparison_func_bool!(compare_gte_bool, op_gte);
make_comparison_func_bool!(compare_lte_bool, op_lte);
