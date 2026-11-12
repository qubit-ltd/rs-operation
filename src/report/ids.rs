// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Stable identities and receipts used by report APIs.

mod clock_domain_id;
mod operation_id;
mod recovery_key;
mod release_receipt;
mod resource_id;
mod transfer_receipt;

pub use clock_domain_id::ClockDomainId;
pub use operation_id::OperationId;
pub use recovery_key::RecoveryKey;
pub use release_receipt::ReleaseReceipt;
pub use resource_id::ResourceId;
pub use transfer_receipt::TransferReceipt;
