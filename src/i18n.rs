//! Lightweight UI-language helper for the DB plugin.
//!
//! The host injects the plugin's effective UI language via the `MPE_LOCALE`
//! environment variable at spawn: the host language when it matches the
//! plugin manifest's `locales`, otherwise the manifest's `default_locale`.
//! This plugin declares `locales: ["zh-CN", "en-US"]` with
//! `default_locale: "en-US"`.

use mpe_plugin_sdk::prelude::locale;

/// Picks the Chinese or English text based on the injected `MPE_LOCALE`.
///
/// `zh` is used only when the effective language is `zh-CN`; every other
/// value (including a missing variable) falls back to `en`. Both arguments
/// must be string literals (they are returned as `&'static str`).
pub fn t(zh: &'static str, en: &'static str) -> &'static str {
    pick(zh, en, locale().as_deref())
}

/// Picks `zh` when `locale` is `zh-CN`, else `en`. Pure decision helper so
/// the locale-dependent choice is testable without touching the global
/// environment.
fn pick(zh: &'static str, en: &'static str, locale: Option<&str>) -> &'static str {
    if locale == Some("zh-CN") {
        zh
    } else {
        en
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default (no MPE_LOCALE) → English.
    #[test]
    fn defaults_to_english() {
        assert_eq!(t("中文", "English"), "English");
    }

    /// zh-CN → Chinese.
    #[test]
    fn zh_locale_selects_chinese() {
        assert_eq!(pick("中文", "English", Some("zh-CN")), "中文");
    }

    /// Other / missing locales fall back to English.
    #[test]
    fn non_matching_locale_falls_back_to_english() {
        assert_eq!(pick("中文", "English", Some("en-US")), "English");
        assert_eq!(pick("中文", "English", Some("fr-FR")), "English");
        assert_eq!(pick("中文", "English", None), "English");
    }
}
