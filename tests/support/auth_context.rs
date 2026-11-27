use super::{Arc, AuthContext, SignedToken};

impl AuthContext {
    pub(crate) fn with_signed(mut self, signed: SignedToken) -> Self {
        self.signed = Some(Arc::new(signed));
        self
    }
}
