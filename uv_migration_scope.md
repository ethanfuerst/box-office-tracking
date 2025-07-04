# UV Migration Scope for Box Office Tracking Project

## Executive Summary

This document outlines the migration plan from Poetry to `uv` for the box-office-tracking project. The migration will modernize the dependency management while maintaining all current functionality.

## Current Project Analysis

### Project Overview
- **Type**: Python application (package-mode: false)
- **Current Package Manager**: Poetry 2.1.3
- **Python Version Requirement**: ^3.10 (pyproject.toml) vs 3.9.7 (.python-version) - **discrepancy needs resolution**
- **Dependencies**: 18 main production dependencies
- **Development Setup**: Pre-commit hooks, Black formatter, SQLFluff linter
- **Deployment**: Modal serverless platform

### Current Dependencies
**Main Dependencies:**
- `gspread` (^5.12.4) - Google Sheets integration
- `pandas` (^2.2.0) - Data manipulation
- `duckdb` (^1.1.3) - Database operations
- `modal` (^1.0.0) - Serverless deployment
- `python-dotenv` (^1.0.1) - Environment variables
- `sqlfluff` (^3.3.0) - SQL linting
- `lxml` (^5.3.0) - XML parsing

**Key Considerations:**
- No development dependencies defined in separate groups
- Uses caret versioning (^) throughout
- No private package repositories
- Standard PyPI dependencies only

## Migration Benefits

### Performance Improvements
- **10-100x faster** dependency resolution compared to Poetry
- **Faster installation** times for packages
- **Reduced memory usage** during operations

### Tooling Simplification
- **Unified Python management**: Eliminates need for separate Python version managers
- **Modern standards**: Better PEP 621 compliance
- **Active development**: Regular updates and improvements from Astral team

### Ecosystem Benefits
- **Better CI/CD integration**: Faster builds and deployments
- **Improved developer experience**: Single tool for most Python needs
- **Future-proofing**: Built for modern Python development workflows

## Migration Strategy

### Recommended Approach: `migrate-to-uv` Tool
**Rationale**: Best suited for this project because:
- Standard dependencies (no private repositories)
- Straightforward dependency structure
- Minimal custom Poetry configuration
- Application project (not library)

### Step-by-Step Migration Plan

#### Phase 1: Preparation (Estimated: 1-2 hours)
1. **Install uv**
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   # or
   pip install uv
   ```

2. **Backup Current State**
   ```bash
   cp pyproject.toml pyproject.toml.backup
   cp poetry.lock poetry.lock.backup
   ```

3. **Resolve Python Version Discrepancy**
   - Decision needed: Use Python 3.9.7 or 3.10+
   - Update either `.python-version` or `pyproject.toml` accordingly
   - **Recommendation**: Upgrade to Python 3.10+ for better performance

#### Phase 2: Migration (Estimated: 30 minutes)
1. **Run Migration Tool**
   ```bash
   uvx migrate-to-uv
   ```

2. **Generate Lock File**
   ```bash
   uv lock
   ```

3. **Test Installation**
   ```bash
   uv sync
   ```

#### Phase 3: Verification (Estimated: 1-2 hours)
1. **Dependency Verification**
   ```bash
   uv tree  # Verify dependency tree
   uv run python -c "import gspread, pandas, duckdb"  # Test imports
   ```

2. **Tool Configuration Updates**
   - Update any scripts that reference `poetry run`
   - Verify Black and SQLFluff still work
   - Test pre-commit hooks

3. **Modal Deployment Testing**
   ```bash
   # Test main script locally first
   uv run python sync_and_update.py --dry-run
   
   # Update Modal deployment files
   # 1. Update sync_and_update.py Modal image configuration
   # 2. Update deploy_modal.sh script
   
   # Test Modal deployment
   uv run modal deploy sync_and_update.py
   ```

#### Phase 4: Cleanup (Estimated: 15 minutes)
1. **Remove Poetry Files** (after successful verification)
   ```bash
   rm poetry.lock
   rm -rf .venv  # If using Poetry-managed venv
   ```

2. **Update Documentation**
   - Update README.md installation instructions
   - Update any developer setup documentation

## Expected Changes

### File Modifications

#### `pyproject.toml`
**Before (Poetry format):**
```toml
[tool.poetry]
name = "box-office-tracking"
version = "0.1.0"
# ... other metadata

[tool.poetry.dependencies]
python = "^3.10"
gspread = "^5.12.4"
# ... other deps
```

**After (uv format):**
```toml
[project]
name = "box-office-tracking"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "gspread>=5.12.4,<6.0.0",
    # ... other deps
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

#### New Files
- `uv.lock` - Replaces `poetry.lock`
- `.python-version` - May be updated for consistency

#### Modified Files
- `sync_and_update.py` - Update Modal image from Poetry to pip method
- `deploy_modal.sh` - Change `poetry run` to `uv run`

#### Removed Files
- `poetry.lock` (after successful migration)

### Command Changes
| Poetry Command | uv Equivalent |
|----------------|---------------|
| `poetry install` | `uv sync` |
| `poetry add package` | `uv add package` |
| `poetry run python script.py` | `uv run python script.py` |
| `poetry shell` | `uv shell` |
| `poetry export` | `uv export` |

## Potential Challenges & Mitigation

### 1. Python Version Discrepancy
**Issue**: `.python-version` (3.9.7) vs `pyproject.toml` (^3.10)
**Mitigation**: 
- Test project with Python 3.10
- Update `.python-version` to 3.10.x
- Ensure Modal deployment supports Python 3.10

### 2. Dependency Resolution Differences
**Issue**: uv might resolve different versions than Poetry
**Mitigation**:
- Compare resolved versions before/after migration
- Test all functionality thoroughly
- Keep Poetry backup for rollback if needed

### 3. Modal Deployment Integration ⚠️ **CRITICAL**
**Issue**: Modal image uses Poetry-specific methods that need replacement
**Found in sync_and_update.py**:
```python
modal_image = (
    modal.Image.debian_slim(python_version='3.10')
    .poetry_install_from_file(poetry_pyproject_toml='pyproject.toml')  # NEEDS CHANGE
    # ...
)
```

**Mitigation**:
- Replace `.poetry_install_from_file()` with `.pip_install_from_pyproject()` 
- Update `deploy_modal.sh` script from `poetry run` to `uv run`
- Test Modal deployment thoroughly with new uv configuration
- Verify Modal platform supports uv-generated lockfiles

**Updated Modal Image Code**:
```python
modal_image = (
    modal.Image.debian_slim(python_version='3.10')
    .pip_install_from_pyproject('pyproject.toml')  # uv-compatible method
    .add_local_dir('config/', remote_path='/root/config')
    .add_local_dir('assets/', remote_path='/root/assets')
    .add_local_python_source('boxofficemojo_etl', 'dashboard_etl', 'utils')
)
```

### 4. CI/CD Pipeline Updates
**Issue**: Any CI/CD using Poetry commands will break
**Mitigation**:
- Update all workflow files to use uv commands
- Test CI/CD pipeline in staging environment

## Testing Checklist

### Core Functionality
- [ ] Google Sheets integration (`gspread` operations)
- [ ] Data processing (`pandas` operations)
- [ ] Database operations (`duckdb` queries)
- [ ] Environment variable loading (`.env` files)
- [ ] Modal deployment functionality

### Development Tools
- [ ] Code formatting with Black
- [ ] SQL linting with SQLFluff
- [ ] Pre-commit hooks execution
- [ ] Python imports and module resolution

### Deployment
- [ ] Modal deployment script works
- [ ] Environment variables properly loaded
- [ ] All dependencies available in Modal environment

## Risk Assessment

### Low Risk ✅
- **Standard dependencies**: All dependencies are from PyPI
- **Application project**: No publishing concerns
- **Modern Python**: Compatible with uv requirements

### Medium Risk ⚠️
- **Modal integration**: Critical code changes required in `sync_and_update.py`
- **Deployment script updates**: `deploy_modal.sh` needs modification  
- **Version resolution**: May get different dependency versions
- **Tool configuration**: Some tools may need reconfiguration

### High Risk ❌
- **None identified** for this project

## Timeline Estimation

| Phase | Duration | Effort Level |
|-------|----------|--------------|
| Preparation | 1-2 hours | Low |
| Migration | 30 minutes | Low |
| Verification | 1-2 hours | Medium |
| Cleanup | 15 minutes | Low |
| **Total** | **3-5 hours** | **Low-Medium** |

## Rollback Plan

If issues arise during migration:

1. **Immediate Rollback**
   ```bash
   cp pyproject.toml.backup pyproject.toml
   cp poetry.lock.backup poetry.lock
   poetry install
   ```

2. **Clean Rollback**
   ```bash
   git checkout -- pyproject.toml  # If using git
   rm uv.lock
   poetry install
   ```

## Recommendations

### 1. Proceed with Migration ✅
**Rationale**: 
- Project is well-suited for uv migration
- Low complexity and risk
- Significant performance benefits
- Future-proofing for modern Python development

### 2. Python Version Strategy
**Recommendation**: Upgrade to Python 3.10+
- Resolve version discrepancy
- Better performance and features
- Longer support lifecycle

### 3. Migration Timing
**Recommendation**: Perform during low-activity period
- Test thoroughly in development first
- Have backup deployment ready
- Coordinate with any dependent systems

### 4. Documentation Updates
**Immediate**: Update developer setup instructions
**Future**: Consider documenting migration lessons learned

## Conclusion

The migration from Poetry to uv for this project is **low-risk** and **highly recommended**. The project's straightforward dependency structure and standard Python packages make it an ideal candidate for migration. The performance benefits and modern tooling approach will improve the developer experience and future maintainability.

**Next Steps:**
1. Resolve Python version discrepancy
2. Schedule migration during appropriate time window
3. Execute migration plan in development environment first
4. Perform thorough testing before production deployment

---

*Document Version: 1.0*
*Created: January 2025*
*Project: box-office-tracking*