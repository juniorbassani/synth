/// bindings for our koto-derived language
///
/// for now we use koto wholesale, will add custom syntax later
// re-export Value for derive macro
pub use koto::runtime::Value;
use koto::runtime::{runtime_error, RuntimeError, ValueNumber};

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

pub enum KeyPath<'parent> {
    Index(usize, Option<&'parent KeyPath<'parent>>),
    Field(Cow<'static, str>, Option<&'parent KeyPath<'parent>>),
}

impl Display for KeyPath<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            &KeyPath::Index(i, parent) => {
                if let Some(p) = parent {
                    write!(f, "{}@{}", p, i)
                } else {
                    write!(f, "{}", i)
                }
            }
            &KeyPath::Field(ref i, parent) => {
                if let Some(p) = parent {
                    write!(f, "{}.{}", p, i)
                } else {
                    write!(f, "{}", i)
                }
            }
        }
    }
}

pub trait FromValue: Sized {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError>;
}

#[cold]
pub fn fn_type_error<T: Sized>(
    fn_name: &str,
    inputs: &str,
    args: &[Value],
) -> Result<T, RuntimeError> {
    let mut types = args.iter().map(|v| v.type_as_string());
    let mut argspec = types.next().unwrap_or_else(String::new);
    for ty in types {
        argspec += ", ";
        argspec += &ty;
    }
    runtime_error!("expected {}({:?}), got {}", fn_name, inputs, argspec,)
}

/// Return an error for a missing item
#[cold]
pub fn missing<T: Sized>(key_path: &KeyPath<'_>) -> Result<T, RuntimeError> {
    runtime_error!("Missing value at {}", key_path)
}

/// Return an error for an item of the wrong type
#[cold]
fn wrong_type<T: Sized>(
    ty: &'static str,
    key_path: &KeyPath<'_>,
    value: &Value,
) -> Result<T, RuntimeError> {
    runtime_error!(
        "expected value of type {} at {}, found {}",
        ty,
        key_path,
        value.type_as_string(),
    )
}

impl FromValue for bool {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
        if let Value::Bool(b) = value {
            Ok(*b)
        } else {
            wrong_type("bool", key_path, value)
        }
    }
}

macro_rules! impl_from_value_num {
    (one $ty:ty, $value:path, $category:expr) => {
        impl FromValue for $ty {
            fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
                if let Value::Number($value(i)) = value {
                    Ok((*i) as $ty)
                } else {
                    wrong_type($category, key_path, value)
                }
            }
        }
    };
    ($category:expr, $value:path, $($ty:ty),*) => {
        $(impl_from_value_num!(one $ty, $value, $category);)*
    }
}

impl_from_value_num!(
    "integer",
    ValueNumber::I64,
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize
);
impl_from_value_num!("float", ValueNumber::F64, f32, f64);

impl FromValue for String {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
        if let Value::Str(s) = value {
            Ok(s.as_str().to_owned())
        } else {
            wrong_type("string", key_path, value)
        }
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
        if let Value::List(elems) = value {
            elems
                .data()
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let item = T::from_value(&KeyPath::Index(i, Some(key_path)), v);
                    item
                })
                .collect()
        } else {
            wrong_type("list of items", key_path, value)
        }
    }
}

impl<K: FromValue + PartialOrd + Ord, V: FromValue> FromValue for BTreeMap<K, V> {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
        if let Value::Map(map) = value {
            map.contents()
                .data
                .iter()
                .map(|(k, v)| {
                    let kval = k.value();
                    Ok((
                        K::from_value(key_path, kval)?,
                        V::from_value(&KeyPath::Field(kval.to_string().into(), Some(key_path)), v)?,
                    ))
                })
                .collect()
        } else {
            wrong_type("map", key_path, value)
        }
    }
}

impl<T: FromValue, E: Sized> FromValue for Result<T, E> {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
        T::from_value(&KeyPath::Field(Cow::Borrowed("ok"), Some(key_path)), value).map(Ok)
    }
}

impl<T, E> IntoValue for Result<T, E>
where
    T: IntoValue,
    E: Error,
{
    fn into_value(self) -> ValueResult {
        match self {
            Ok(ok) => ok.into_value(),
            Err(e) => runtime_error!("{}", e),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(key_path: &KeyPath<'_>, value: &Value) -> Result<Self, RuntimeError> {
        if let &Value::Empty = value {
            Ok(None)
        } else {
            T::from_value(key_path, value).map(Some)
        }
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> ValueResult {
        self.map_or(Ok(Value::Empty), IntoValue::into_value)
    }
}

type ValueResult = Result<Value, RuntimeError>;

/// Make a koto Value out of some Rust value
pub trait IntoValue: Sized {
    fn into_value(self) -> ValueResult;
}

impl IntoValue for () {
    fn into_value(self) -> ValueResult {
        Ok(Value::Empty)
    }
}

impl IntoValue for bool {
    fn into_value(self) -> ValueResult {
        Ok(Value::Bool(self))
    }
}

impl<T: Clone + IntoValue> IntoValue for &T {
    fn into_value(self) -> ValueResult {
        self.clone().into_value()
    }
}

impl IntoValue for String {
    fn into_value(self) -> ValueResult {
        Ok(Value::Str(self.into()))
    }
}

impl IntoValue for &str {
    fn into_value(self) -> ValueResult {
        Ok(Value::Str(self.into()))
    }
}

macro_rules! impl_into_value_num {
    (one $ty:ty, $as_ty:ty, $variant:path) => {
        impl IntoValue for $ty {
            fn into_value(self) -> ValueResult {
                Ok(Value::Number($variant(self as $as_ty)))
            }
        }
    };
    ($variant:path, $as_ty:ty; $($tys:ty),*) => {
        $(
            impl_into_value_num!(one $tys, $as_ty, $variant);
        )*
    };
}

impl_into_value_num!(ValueNumber::I64, i64; u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);
impl_into_value_num!(ValueNumber::F64, f64; f32, f64);
