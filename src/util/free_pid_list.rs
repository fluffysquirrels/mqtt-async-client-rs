use maplit::btreemap;
use std::collections::BTreeMap;

pub struct FreePidList {
    /// A map of non-overlapping free intervals where the key is the
    /// inclusive lower bound and the value is the inclusive upper
    /// bound.
    map: BTreeMap<u16, u16>,

    /// Inclusive lower bound of available pids.
    lb: u16,

    /// Inclusive upper bound of available pids.
    ub: u16,
}

impl FreePidList {
    /// Returns a new instance with all pids available.
    pub fn new() -> FreePidList {
        Self::with_bounds(1, std::u16::MAX)
    }

    pub fn with_bounds(lb: u16, ub: u16) -> FreePidList {
        assert!(lb <= ub, "lb <= ub");
        assert!(lb >= 1, "lb >= 1");
        FreePidList {
            lb, ub,
            map: btreemap!{ lb => ub },
        }
    }

    #[allow(dead_code)]
    /// Resets this instance by marking all pids available.
    pub fn clear(&mut self) {
        self.map = btreemap!{ self.lb => self.ub }
    }

    /// Allocates a new Pid.
    ///
    /// Returns None if no free Pids are available, otherwise returns
    /// Some(pid) and marks pid as used.
    pub fn alloc(&mut self) -> Option<u16> {
        let range = self.map.iter().next();
        let (lb, ub) = match range {
            None => return None,
            Some(x) => (*x.0, *x.1),
        };
        let ret = lb;
        self.map.remove(&lb);
        if ub > lb {
            self.map.insert(lb + 1, ub);
        }
        assert!(ret >= self.lb && ret <= self.ub, "ret >= self.lb && ret <= self.ub");
        Some(ret)
    }

    /// Returns true if Pid was already free.
    /// TODO: Better return type?
    ///
    /// Panics if x == 0.
    pub fn free(&mut self, x: u16) -> bool {
        assert!(x >= self.lb, "x >= lb");
        assert!(x <= self.ub, "x <= ub");

        let range_above: Option<Range> =
            self.map.range(x..=(self.ub))
                .next().map(|(kr, vr)| Range::from((*kr, *vr)));
        let range_below: Option<Range> =
            self.map.range((self.lb)..=x)
                .next().map(|(kr, vr)| Range::from((*kr, *vr)));

        if (range_above.is_some() && range_above.unwrap().contains(x)) ||
           (range_below.is_some() && range_below.unwrap().contains(x))
        {
            return true;
        }

        let range_above_merges =
            range_above.is_some() &&
            x < std::u16::MAX && // Check x+1 below won't overflow
            range_above.unwrap().lb == x+1;

        // x >= 1 by assertion above so x-1 won't underflow.
        let range_below_merges =
            range_below.is_some() &&
            range_below.unwrap().ub == x-1;

        // 4 different cases for range_{above,below}_merges each being true or false
        if range_above_merges && range_below_merges {
            let range_above = range_above.unwrap();
            let range_below = range_below.unwrap();
            self.map.remove(&range_above.lb);
            self.map.remove(&range_below.lb);
            self.map.insert(range_below.lb, range_above.ub);
        } else if range_above_merges && !range_below_merges {
            let range_above = range_above.unwrap();
            self.map.remove(&range_above.lb);
            self.map.insert(x, range_above.ub);
        } else if !range_above_merges && range_below_merges {
            let range_below = range_below.unwrap();
            self.map.remove(&range_below.lb);
            self.map.insert(range_below.lb, x);
        } else if !range_above_merges && !range_below_merges {
            self.map.insert(x, x);
        } else {
            panic!("Not reached");
        }
        false
    }
}

#[derive(Clone, Copy)]
struct Range {
    /// Lower bound, inclusive.
    lb: u16,

    /// Upper bound, inclusive.
    ub: u16,
}

impl From<(u16, u16)> for Range {
    fn from(o: (u16, u16)) -> Range {
        Range { lb: o.0, ub: o.1 }
    }
}

impl Range {
    fn contains(&self, p: u16) -> bool {
        self.lb <= p && self.ub >= p
    }
}

#[cfg(test)]
mod tests {
    use maplit::btreemap;
    use super::FreePidList;

    #[test]
    fn ex_1() {
        let mut l = FreePidList::new();
        assert_eq!(l.map, btreemap!{1 => std::u16::MAX});

        let a = l.alloc().unwrap();
        assert_eq!(a, 1);
        assert_eq!(l.map, btreemap!{2 => std::u16::MAX});

        let b = l.alloc().unwrap();
        assert_eq!(b, 2);
        assert_eq!(l.map, btreemap!{3 => std::u16::MAX});

        assert_eq!(l.free(a), false);
        assert_eq!(l.map, btreemap!{1 => 1, 3 => std::u16::MAX});

        let a = l.alloc().unwrap();
        assert_eq!(a, 1);
        assert_eq!(l.map, btreemap!{3 => std::u16::MAX});

        assert_eq!(l.free(b), false);
        assert_eq!(l.map, btreemap!{2 => std::u16::MAX});

        assert_eq!(l.free(a), false);
        assert_eq!(l.map, btreemap!{1 => std::u16::MAX});
    }

    #[test]
    fn empty() {
        let mut l = FreePidList::new();
        for _ in 1..=std::u16::MAX {
            l.alloc().unwrap();
        }

        assert_eq!(l.map, btreemap!{});
        assert_eq!(l.alloc(), None);

        assert_eq!(l.map, btreemap!{});
        l.free(1);
        assert_eq!(l.map, btreemap!{1 => 1});
        l.alloc().unwrap();

        assert_eq!(l.map, btreemap!{});
        l.free(2);
        assert_eq!(l.map, btreemap!{2 => 2});
        l.alloc().unwrap();

        assert_eq!(l.map, btreemap!{});
        l.free(std::u16::MAX);
        assert_eq!(l.map, btreemap!{std::u16::MAX => std::u16::MAX});
        l.alloc().unwrap();
        assert_eq!(l.map, btreemap!{});
    }

    #[test]
    fn free_case_merge_below() {
        let mut l = FreePidList::new();
        l.alloc().unwrap();
        l.alloc().unwrap();
        l.alloc().unwrap();
        l.free(1);
        assert_eq!(l.map, btreemap!{1 => 1, 4 => std::u16::MAX});
        l.free(2);
        assert_eq!(l.map, btreemap!{1 => 2, 4 => std::u16::MAX});
    }

    #[test]
    fn free_case_merge_above() {
        let mut l = FreePidList::new();
        l.alloc().unwrap();
        l.alloc().unwrap();
        l.alloc().unwrap();
        l.free(1);
        assert_eq!(l.map, btreemap!{1 => 1, 4 => std::u16::MAX});
        l.free(3);
        assert_eq!(l.map, btreemap!{1 => 1, 3 => std::u16::MAX});
    }

    #[test]
    fn free_case_merge_above_and_below() {
        let mut l = FreePidList::new();
        l.alloc().unwrap();
        l.alloc().unwrap();
        l.free(1);
        assert_eq!(l.map, btreemap!{1 => 1, 3 => std::u16::MAX});
        l.free(2);
        assert_eq!(l.map, btreemap!{1 => std::u16::MAX});
    }

    #[test]
    fn free_case_new_range() {
        let mut l = FreePidList::new();
        l.alloc().unwrap();
        l.alloc().unwrap();
        assert_eq!(l.map, btreemap!{3 => std::u16::MAX});
        l.free(1);
        assert_eq!(l.map, btreemap!{1 => 1, 3 => std::u16::MAX});
    }

    #[test]
    fn double_free_lower_bound() {
        let mut l = FreePidList::new();
        assert_eq!(l.map, btreemap!{1 => std::u16::MAX});
        assert_eq!(l.free(1), true);
    }

    #[test]
    fn double_free_upper_bound() {
        let mut l = FreePidList::new();
        assert_eq!(l.map, btreemap!{1 => std::u16::MAX});
        assert_eq!(l.free(std::u16::MAX), true);
    }

    #[test]
    fn clear() {
        let mut l = FreePidList::new();
        l.alloc().unwrap();
        assert_eq!(l.map, btreemap!{2 => std::u16::MAX});
        l.clear();
        assert_eq!(l.map, btreemap!{1 => std::u16::MAX});
    }

    #[test]
    #[should_panic]
    fn bad_bounds_swapped() {
        FreePidList::with_bounds(10, 5);
    }

    #[test]
    #[should_panic]
    fn bad_bounds_zero() {
        FreePidList::with_bounds(0, 5);
    }

    #[test]
    fn bounds_ex() {
        let mut l = FreePidList::with_bounds(1, 2);
        assert_eq!(l.map, btreemap! {1 => 2});
        assert_eq!(l.alloc(), Some(1));
        assert_eq!(l.map, btreemap! {2 => 2});
        assert_eq!(l.alloc(), Some(2));
        assert_eq!(l.map, btreemap! {});
        assert_eq!(l.alloc(), None);
        assert_eq!(l.free(1), false);
        assert_eq!(l.map, btreemap! {1 => 1});
        assert_eq!(l.free(2), false);
        assert_eq!(l.map, btreemap! {1 => 2});
        assert_eq!(l.free(2), true);
    }

    #[test]
    #[should_panic]
    fn bounds_bad_free_lb() {
        let mut l = FreePidList::with_bounds(5, 10);
        l.free(1);
    }

    #[test]
    #[should_panic]
    fn bounds_bad_free_ub() {
        let mut l = FreePidList::with_bounds(5, 10);
        l.free(11);
    }
}
